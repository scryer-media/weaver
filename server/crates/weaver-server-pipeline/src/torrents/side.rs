// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's side store: where a verified piece keeps what it holds of the
// files the torrent does not download, so those files are never written by
// a piece they only share (libtorrent `part_file`, written and read in place
// of a `dont_download` file by `mmap_storage::writev` and `readv`,
// mmap_storage.cpp:555-575 and 655-670 at v2.0.15).
//
// It is one file in Weaver's working directory, made at the first piece it
// keeps. A head comes first: the files whose shares of the pieces kept may
// be here alone, one bit each. Every file set aside when a piece is kept is
// among them, and a file leaves them only once its shares are copied out,
// so a store opened under other levels knows which shares to copy out.
// Slots follow, each a tag then one piece's bytes whole, as the piece was
// hashed. The tag says which piece the slot holds, or that it is free; it
// is written after the bytes, and the piece is had only after both, so a
// slot whose tag is there holds what it says. Slots are found by their tags
// when the store opens; only pieces that cross into a file set aside take
// one, a piece or two for each such file's ends, so the file stays small.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::files::{read_exact_at, write_all_at};

const MAGIC: u32 = 0x5744_5353;
const HEAD_MAGIC: u32 = 0x5744_5348;
const TAG: u64 = 8;

pub(crate) struct Side {
    // None: no side store, and no file is set aside.
    path: Option<PathBuf>,
    slot_len: u64,
    // Where the first slot starts, after the head.
    base: u64,
    // The files the head names, those whose shares may be here alone.
    recorded: Vec<bool>,
    // The selection of files set aside the head was last found to name in
    // full: a piece kept under that same selection writes no head, and
    // its files are not scanned again. Held, so its address is never
    // another selection's while it is here.
    covered: Option<Arc<Vec<bool>>>,
    // How many times a piece kept scanned the files set aside.
    #[cfg(test)]
    scans: usize,
    // Whether the file is there, its head written.
    made: bool,
    // The slot each piece kept here is in.
    slots: HashMap<u32, u32>,
    // The slot each piece still arriving block by block is written into,
    // untagged until it verifies (`seal`). None is found at open: a piece
    // still arriving when the process stopped is fetched again.
    pending: HashMap<u32, u32>,
    // Slots no piece is in, in the file already.
    free: Vec<u32>,
    // How many slots the file has room for.
    count: u32,
    // Why the file found there was not this torrent's side store, when it
    // was not.
    reset: Option<&'static str>,
}

// The head's length for `files` files: the magic, the count, the bits,
// rounded up to the tag's length.
fn head_len(files: usize) -> u64 {
    (8 + (files as u64).div_ceil(8)).next_multiple_of(TAG)
}

fn head(recorded: &[bool]) -> Vec<u8> {
    let mut head = vec![0; head_len(recorded.len()) as usize];
    head[..4].copy_from_slice(&HEAD_MAGIC.to_le_bytes());
    head[4..8].copy_from_slice(&(recorded.len() as u32).to_le_bytes());
    for (file, set) in recorded.iter().enumerate() {
        if *set {
            head[8 + file / 8] |= 1 << (file % 8);
        }
    }
    head
}

fn tag(piece: Option<u32>) -> [u8; TAG as usize] {
    let mut tag = [0; TAG as usize];
    if let Some(piece) = piece {
        tag[..4].copy_from_slice(&(piece + 1).to_le_bytes());
        tag[4..].copy_from_slice(&MAGIC.to_le_bytes());
    }
    tag
}

fn untag(tag: [u8; TAG as usize]) -> Option<u32> {
    let piece = u32::from_le_bytes(tag[..4].try_into().ok()?);
    let magic = u32::from_le_bytes(tag[4..].try_into().ok()?);
    (magic == MAGIC && piece > 0).then(|| piece - 1)
}

fn open_for_write(path: &Path) -> io::Result<File> {
    let open = || {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
    };
    match open() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            open()
        }
        opened => opened,
    }
}

impl Side {
    // No side store: every file is written as it is.
    pub(crate) fn none() -> Self {
        Self {
            path: None,
            slot_len: 0,
            base: 0,
            recorded: Vec::new(),
            covered: None,
            #[cfg(test)]
            scans: 0,
            made: false,
            slots: HashMap::new(),
            pending: HashMap::new(),
            free: Vec::new(),
            count: 0,
            reset: None,
        }
    }

    fn empty(piece_length: u64, files: usize) -> Self {
        Self {
            slot_len: TAG + piece_length,
            base: head_len(files),
            recorded: vec![false; files],
            ..Self::none()
        }
    }

    // The side store at `path` for pieces of `piece_length` over `files`
    // files, its slots found by their tags. None of it there is an empty
    // store. A file there that is not this torrent's side store, its head
    // short or another's, or longer than any it makes, is removed and the
    // store is empty, with the reason in `reset`: nothing of it is read
    // back as a slot, and the next piece kept lays down a fresh head
    // (libtorrent takes such a part file as empty and writes over it,
    // `part_file::part_file`, part_file.cpp:87-118 at v2.0.15). A file
    // that cannot be opened or read is an error.
    pub(crate) fn open(path: PathBuf, piece_length: u64, files: usize) -> io::Result<Self> {
        let mut side = Self::empty(piece_length, files);
        match File::open(&path) {
            Ok(file) => {
                if let Some(reason) = side.load(&file)? {
                    drop(file);
                    Self::remove(&path)?;
                    side = Self::empty(piece_length, files);
                    side.reset = Some(reason);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        side.path = Some(path);
        Ok(side)
    }

    // The head and slots of `file`, or why it is not this torrent's side
    // store; nothing is taken from it then.
    fn load(&mut self, file: &File) -> io::Result<Option<&'static str>> {
        let len = file.metadata()?.len();
        let mut found = vec![0; self.base as usize];
        match read_exact_at(file, 0, &mut found) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(Some("its head is short"));
            }
            Err(error) => return Err(error),
        }
        let magic = u32::from_le_bytes(found[..4].try_into().unwrap_or_default());
        let count = u32::from_le_bytes(found[4..8].try_into().unwrap_or_default());
        if magic != HEAD_MAGIC || count as usize != self.recorded.len() {
            return Ok(Some("its head is not this torrent's"));
        }
        let Ok(slots) = u32::try_from(len.saturating_sub(self.base).div_ceil(self.slot_len)) else {
            return Ok(Some("it is longer than any side store"));
        };
        for (file, set) in self.recorded.iter_mut().enumerate() {
            *set = found[8 + file / 8] & (1 << (file % 8)) != 0;
        }
        self.made = true;
        self.count = slots;
        for slot in 0..slots {
            let mut found = [0; TAG as usize];
            let at = self.base + u64::from(slot) * self.slot_len;
            let piece = match read_exact_at(file, at, &mut found) {
                Ok(()) => untag(found),
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => None,
                Err(error) => return Err(error),
            };
            match piece {
                Some(piece) if !self.slots.contains_key(&piece) => {
                    self.slots.insert(piece, slot);
                }
                _ => self.free.push(slot),
            }
        }
        Ok(None)
    }

    // Why the file found at open was not this torrent's side store, and was
    // removed, when it was not.
    pub(crate) fn reset(&self) -> Option<&'static str> {
        self.reset
    }

    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    // Whether the piece's bytes of the files set aside are here: kept, or
    // still arriving.
    pub(crate) fn holds(&self, piece: u32) -> bool {
        self.slots.contains_key(&piece) || self.pending.contains_key(&piece)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty() && self.pending.is_empty()
    }

    // The pieces kept here.
    pub(crate) fn pieces(&self) -> Vec<u32> {
        let mut pieces: Vec<u32> = self.slots.keys().copied().collect();
        pieces.sort_unstable();
        pieces
    }

    fn file_path(&self) -> io::Result<&Path> {
        self.path.as_deref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "the torrent has no side store")
        })
    }

    // Whether the head names the file: its shares of the pieces kept may be
    // here alone.
    pub(crate) fn recorded(&self, file: usize) -> bool {
        self.recorded.get(file).copied().unwrap_or(false)
    }

    // The head names the files `aside` names from now on, and no other:
    // only once every share of a file it stops naming is copied out.
    pub(crate) fn record(&mut self, aside: &[bool]) -> io::Result<()> {
        if self.recorded == aside {
            return Ok(());
        }
        if self.made {
            let file = OpenOptions::new().write(true).open(self.file_path()?)?;
            write_all_at(&file, 0, &head(aside))?;
        }
        self.recorded = aside.to_vec();
        self.covered = None;
        Ok(())
    }

    // The piece kept whole, with the files `aside` names set aside: the
    // head naming them, its bytes in a slot, then the slot's tag. The
    // files are scanned once per selection: a piece kept under the
    // selection the head already names in full goes straight to its slot.
    pub(crate) fn keep(
        &mut self,
        piece: u32,
        held: &[u8],
        aside: &Arc<Vec<bool>>,
    ) -> io::Result<()> {
        let (file, slot) = self.open_slot(piece, aside)?;
        let at = self.at(slot);
        write_all_at(&file, at + TAG, held)?;
        write_all_at(&file, at, &tag(Some(piece)))?;
        let arriving = self.pending.remove(&piece).is_some();
        if self.slots.insert(piece, slot).is_none() && !arriving {
            self.taken(slot);
        }
        Ok(())
    }

    // A block of a piece still arriving, its bytes from `at` of the piece
    // in the piece's slot: the slot is the piece's from its first block,
    // its tag cleared, and is tagged only once the piece verified
    // (`seal`), so a slot whose tag is there still holds what it says.
    pub(crate) fn keep_block(
        &mut self,
        piece: u32,
        at: u64,
        bytes: &[u8],
        aside: &Arc<Vec<bool>>,
    ) -> io::Result<()> {
        let (file, slot) = self.open_slot(piece, aside)?;
        let start = self.at(slot);
        if let std::collections::hash_map::Entry::Vacant(pending) = self.pending.entry(piece) {
            // A free slot may still carry the tag of a piece it held.
            write_all_at(&file, start, &tag(None))?;
            pending.insert(slot);
            if !self.slots.contains_key(&piece) {
                self.taken(slot);
            }
        }
        write_all_at(&file, start + TAG + at, bytes)
    }

    // The piece still arriving verified: its slot is tagged, and it is
    // kept here. A piece with no block here has nothing to tag.
    pub(crate) fn seal(&mut self, piece: u32) -> io::Result<()> {
        let Some(slot) = self.pending.get(&piece).copied() else {
            return Ok(());
        };
        let file = OpenOptions::new().write(true).open(self.file_path()?)?;
        write_all_at(&file, self.at(slot), &tag(Some(piece)))?;
        self.pending.remove(&piece);
        self.slots.insert(piece, slot);
        Ok(())
    }

    // Where the slot starts.
    fn at(&self, slot: u32) -> u64 {
        self.base + u64::from(slot) * self.slot_len
    }

    // The slot taken from the free ones, or added past them.
    fn taken(&mut self, slot: u32) {
        if self.free.last() == Some(&slot) {
            self.free.pop();
        } else {
            self.count += 1;
        }
    }

    // The store's file open for writing, its head naming every file
    // `aside` sets aside, and the slot the piece goes in: the one it is
    // in, or the next free one. The files are scanned once per selection.
    fn open_slot(&mut self, piece: u32, aside: &Arc<Vec<bool>>) -> io::Result<(File, u32)> {
        let file = open_for_write(self.file_path()?)?;
        let covered = self.made
            && self
                .covered
                .as_ref()
                .is_some_and(|covered| Arc::ptr_eq(covered, aside));
        if !covered {
            #[cfg(test)]
            {
                self.scans += 1;
            }
            if !self.made
                || self
                    .recorded
                    .iter()
                    .zip(aside.iter())
                    .any(|(was, is)| *is && !was)
            {
                // Every file set aside is named; one named and no longer
                // set aside stays named until its shares are copied out.
                let named: Vec<bool> = self
                    .recorded
                    .iter()
                    .zip(aside.iter())
                    .map(|(was, is)| *was || *is)
                    .collect();
                write_all_at(&file, 0, &head(&named))?;
                self.recorded = named;
                self.made = true;
            }
            self.covered = Some(Arc::clone(aside));
        }
        let slot = match self.pending.get(&piece).or(self.slots.get(&piece)) {
            Some(slot) => *slot,
            None => match self.free.last() {
                Some(slot) => *slot,
                None => self.count,
            },
        };
        Ok((file, slot))
    }

    // `out` from `at` inside the piece kept here, or still arriving here.
    pub(crate) fn read(&self, piece: u32, at: u64, out: &mut [u8]) -> io::Result<()> {
        let slot = self
            .pending
            .get(&piece)
            .or(self.slots.get(&piece))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "the piece is not in the side store",
                )
            })?;
        let file = File::open(self.file_path()?)?;
        read_exact_at(
            &file,
            self.base + u64::from(*slot) * self.slot_len + TAG + at,
            out,
        )
    }

    // The piece kept here no longer: its slot's tag is cleared, and the slot
    // is free for the next. A piece still arriving had no tag written.
    pub(crate) fn drop_piece(&mut self, piece: u32) -> io::Result<()> {
        if let Some(slot) = self.pending.remove(&piece)
            && self.slots.get(&piece) != Some(&slot)
        {
            self.free.push(slot);
        }
        let Some(slot) = self.slots.get(&piece).copied() else {
            return Ok(());
        };
        let file = OpenOptions::new().write(true).open(self.file_path()?)?;
        write_all_at(
            &file,
            self.base + u64::from(slot) * self.slot_len,
            &tag(None),
        )?;
        self.slots.remove(&piece);
        self.free.push(slot);
        Ok(())
    }

    // The side store gone with its torrent; none there is nothing to do.
    pub(crate) fn remove(path: &Path) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(aside: &[bool]) -> Arc<Vec<bool>> {
        Arc::new(aside.to_vec())
    }

    // Keeping pieces under one selection scans its files once, and writes
    // the same file as a scan for every piece did: the same head, slots
    // and names, through a change of selection and a record in between.
    #[test]
    fn a_piece_kept_under_the_same_selection_scans_no_file() {
        const FILES: usize = 40_000;
        let dir = tempfile::tempdir().unwrap();
        let odd = sel(&(0..FILES).map(|file| file % 2 == 1).collect::<Vec<_>>());
        let more = sel(&(0..FILES)
            .map(|file| file % 2 == 1 || file % 3 == 0)
            .collect::<Vec<_>>());
        let fewer = (0..FILES).map(|file| file % 4 == 1).collect::<Vec<_>>();
        // Each step keeps a piece per two files of a stretch, under one
        // selection; `None` records `fewer` in between.
        type Step<'a> = (Option<&'a Arc<Vec<bool>>>, std::ops::Range<u32>);
        let steps: [Step<'_>; 4] = [
            (Some(&odd), 0..1_000),
            (Some(&more), 1_000..1_500),
            (None, 0..0),
            (Some(&more), 1_500..2_000),
        ];
        let run = |name: &str, fresh: bool| {
            let path = dir.path().join(name);
            let mut side = Side::open(path.clone(), 4, FILES).unwrap();
            for (aside, pieces) in &steps {
                let Some(aside) = aside else {
                    side.record(&fewer).unwrap();
                    continue;
                };
                for piece in pieces.clone() {
                    let held = piece.to_le_bytes();
                    // The old path: every piece scanned, as a selection
                    // never seen before.
                    let aside = if fresh { sel(aside) } else { Arc::clone(aside) };
                    side.keep(piece, &held, &aside).unwrap();
                }
            }
            (
                std::fs::read(&path).unwrap(),
                side.recorded.clone(),
                side.scans,
            )
        };
        let (old, old_named, old_scans) = run("old.side", true);
        let (new, new_named, scans) = run("new.side", false);
        assert_eq!(new, old);
        assert_eq!(new_named, old_named);
        assert_eq!(old_scans, 2_000);
        // One scan per selection, and one after the record.
        assert_eq!(scans, 3);
        assert_eq!(
            &new[..head_len(FILES) as usize],
            head(&new_named).as_slice()
        );
    }

    // A piece written block by block is in its slot as it arrives, read
    // there, and untagged until it verifies: opened again before that, the
    // store holds nothing of it; sealed, it is found again by its tag. One
    // that fails gives its slot to the next, and a slot that held a piece
    // loses that piece's tag as a new one arrives in it.
    #[test]
    fn a_piece_arriving_block_by_block_is_tagged_only_once_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work").join("t.side");
        let aside = sel(&[false, true]);
        let mut side = Side::open(path.clone(), 4, 2).unwrap();
        side.keep_block(5, 2, b"cd", &aside).unwrap();
        side.keep_block(5, 0, b"ab", &aside).unwrap();
        assert!(side.holds(5));
        assert!(!side.is_empty());
        let mut out = [0; 4];
        side.read(5, 0, &mut out).unwrap();
        assert_eq!(&out, b"abcd");
        assert!(Side::open(path.clone(), 4, 2).unwrap().pieces().is_empty());

        side.seal(5).unwrap();
        let again = Side::open(path.clone(), 4, 2).unwrap();
        assert_eq!(again.pieces(), [5]);
        again.read(5, 0, &mut out).unwrap();
        assert_eq!(&out, b"abcd");

        // A piece that fails before it verified frees its slot.
        side.keep_block(6, 0, b"wxyz", &aside).unwrap();
        side.drop_piece(6).unwrap();
        assert!(!side.holds(6));
        side.keep_block(7, 0, b"ijkl", &aside).unwrap();
        side.seal(7).unwrap();
        assert_eq!(Side::open(path.clone(), 4, 2).unwrap().pieces(), [5, 7]);
        assert_eq!(side.count, 2);

        // Piece 5 dropped, its slot taken by 8 arriving: a restart before 8
        // verifies finds neither.
        side.drop_piece(5).unwrap();
        side.keep_block(8, 0, b"mn", &aside).unwrap();
        assert_eq!(Side::open(path.clone(), 4, 2).unwrap().pieces(), [7]);
        assert_eq!(side.count, 2);
    }

    // A piece kept reads back from its slot, a dropped one's slot is taken
    // by the next, and the store opened again finds what it holds by the
    // tags alone, and by its head which files' shares may be there alone.
    #[test]
    fn pieces_kept_are_found_again_by_their_tags() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work").join("t.side");
        let mut side = Side::open(path.clone(), 4, 2).unwrap();
        assert!(!path.exists());
        side.keep(7, b"abcd", &sel(&[false, true])).unwrap();
        side.keep(2, b"efgh", &sel(&[false, true])).unwrap();
        side.drop_piece(7).unwrap();
        // A file no longer set aside stays named until it is recorded so.
        side.keep(9, b"ijkl", &sel(&[true, false])).unwrap();
        let mut again = Side::open(path.clone(), 4, 2).unwrap();
        assert_eq!(again.pieces(), [2, 9]);
        assert!(again.recorded(0) && again.recorded(1));
        let mut out = [0; 2];
        again.read(9, 2, &mut out).unwrap();
        assert_eq!(&out, b"kl");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            head_len(2) + 2 * (TAG + 4)
        );
        again.record(&[true, false]).unwrap();
        let again = Side::open(path.clone(), 4, 2).unwrap();
        assert!(again.recorded(0) && !again.recorded(1));
        assert!(again.reset().is_none());
        Side::remove(&path).unwrap();
        Side::remove(&path).unwrap();
        assert!(!path.exists());
    }

    // A file that is not this torrent's side store opens as an empty one:
    // its head short, another's magic, or another count of files. It is
    // removed, so none of its slots is read back later, even one whose tag
    // a fresh head would make look kept, and the next piece kept lays down
    // a fresh head.
    #[test]
    fn a_file_that_is_not_this_side_store_opens_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.side");
        let mut other = Side::open(path.clone(), 4, 3).unwrap();
        other.keep(5, b"abcd", &sel(&[false, true, false])).unwrap();
        other.keep(6, b"efgh", &sel(&[false, true, false])).unwrap();
        let three = std::fs::read(&path).unwrap();
        // The same slots under a two-file head, its magic broken.
        let mut magic = head(&[false, true]);
        magic[0] ^= 1;
        magic.extend_from_slice(&three[head_len(3) as usize..]);
        let cases: [(&str, Vec<u8>); 3] = [
            ("its head is short", vec![0x48, 0x53, 0x44]),
            ("its head is not this torrent's", magic),
            ("its head is not this torrent's", three),
        ];
        for (reason, bytes) in cases {
            std::fs::write(&path, &bytes).unwrap();
            let mut side = Side::open(path.clone(), 4, 2).unwrap();
            assert_eq!(side.reset(), Some(reason));
            assert!(side.is_empty());
            assert!(!side.recorded(0) && !side.recorded(1));
            assert!(!path.exists());
            side.keep(6, b"ijkl", &sel(&[false, true])).unwrap();
            let mut out = [0; 4];
            side.read(6, 0, &mut out).unwrap();
            assert_eq!(&out, b"ijkl");
            let again = Side::open(path.clone(), 4, 2).unwrap();
            assert!(again.reset().is_none());
            assert_eq!(again.pieces(), [6]);
            assert!(again.recorded(1));
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                head_len(2) + TAG + 4
            );
        }
    }

    // A file that cannot be read as a side store at all, here a folder, is
    // an error, and is left where it is.
    #[test]
    fn a_side_store_that_cannot_be_read_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.side");
        std::fs::create_dir(&path).unwrap();
        assert!(Side::open(path.clone(), 4, 2).is_err());
        assert!(path.is_dir());
    }
}
