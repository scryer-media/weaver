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
    // Whether the file is there, its head written.
    made: bool,
    // The slot each piece kept here is in.
    slots: HashMap<u32, u32>,
    // Slots no piece is in, in the file already.
    free: Vec<u32>,
    // How many slots the file has room for.
    count: u32,
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

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_string())
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
            made: false,
            slots: HashMap::new(),
            free: Vec::new(),
            count: 0,
        }
    }

    // The side store at `path` for pieces of `piece_length` over `files`
    // files, its slots found by their tags. None of it there is an empty
    // store.
    pub(crate) fn open(path: PathBuf, piece_length: u64, files: usize) -> io::Result<Self> {
        let mut side = Self {
            path: None,
            slot_len: TAG + piece_length,
            base: head_len(files),
            recorded: vec![false; files],
            made: false,
            slots: HashMap::new(),
            free: Vec::new(),
            count: 0,
        };
        match File::open(&path) {
            Ok(file) => {
                let len = file.metadata()?.len();
                let mut found = vec![0; side.base as usize];
                read_exact_at(&file, 0, &mut found)
                    .map_err(|_| bad("the side store has no head"))?;
                let magic = u32::from_le_bytes(found[..4].try_into().unwrap_or_default());
                let count = u32::from_le_bytes(found[4..8].try_into().unwrap_or_default());
                if magic != HEAD_MAGIC || count as usize != files {
                    return Err(bad("the side store's head is not this torrent's"));
                }
                for (file, set) in side.recorded.iter_mut().enumerate() {
                    *set = found[8 + file / 8] & (1 << (file % 8)) != 0;
                }
                side.made = true;
                side.count = u32::try_from((len - side.base).div_ceil(side.slot_len))
                    .map_err(|_| bad("the side store is too long"))?;
                for slot in 0..side.count {
                    let mut found = [0; TAG as usize];
                    let at = side.base + u64::from(slot) * side.slot_len;
                    let piece = match read_exact_at(&file, at, &mut found) {
                        Ok(()) => untag(found),
                        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => None,
                        Err(error) => return Err(error),
                    };
                    match piece {
                        Some(piece) if !side.slots.contains_key(&piece) => {
                            side.slots.insert(piece, slot);
                        }
                        _ => side.free.push(slot),
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        side.path = Some(path);
        Ok(side)
    }

    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub(crate) fn kept(&self, piece: u32) -> bool {
        self.slots.contains_key(&piece)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
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
        Ok(())
    }

    // The piece kept whole, with the files `aside` names set aside: the
    // head naming them, its bytes in a slot, then the slot's tag.
    pub(crate) fn keep(&mut self, piece: u32, held: &[u8], aside: &[bool]) -> io::Result<()> {
        let file = open_for_write(self.file_path()?)?;
        if !self.made || self.recorded.iter().zip(aside).any(|(was, is)| *is && !was) {
            // Every file set aside is named; one named and no longer set
            // aside stays named until its shares are copied out.
            let named: Vec<bool> = self
                .recorded
                .iter()
                .zip(aside)
                .map(|(was, is)| *was || *is)
                .collect();
            write_all_at(&file, 0, &head(&named))?;
            self.recorded = named;
            self.made = true;
        }
        let slot = match self.slots.get(&piece) {
            Some(slot) => *slot,
            None => match self.free.last() {
                Some(slot) => *slot,
                None => self.count,
            },
        };
        let at = self.base + u64::from(slot) * self.slot_len;
        write_all_at(&file, at + TAG, held)?;
        write_all_at(&file, at, &tag(Some(piece)))?;
        if self.slots.insert(piece, slot).is_none() {
            if self.free.last() == Some(&slot) {
                self.free.pop();
            } else {
                self.count += 1;
            }
        }
        Ok(())
    }

    // `out` from `at` inside the piece kept here.
    pub(crate) fn read(&self, piece: u32, at: u64, out: &mut [u8]) -> io::Result<()> {
        let slot = self.slots.get(&piece).ok_or_else(|| {
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
    // is free for the next.
    pub(crate) fn drop_piece(&mut self, piece: u32) -> io::Result<()> {
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

    // A piece kept reads back from its slot, a dropped one's slot is taken
    // by the next, and the store opened again finds what it holds by the
    // tags alone, and by its head which files' shares may be there alone.
    #[test]
    fn pieces_kept_are_found_again_by_their_tags() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work").join("t.side");
        let mut side = Side::open(path.clone(), 4, 2).unwrap();
        assert!(!path.exists());
        side.keep(7, b"abcd", &[false, true]).unwrap();
        side.keep(2, b"efgh", &[false, true]).unwrap();
        side.drop_piece(7).unwrap();
        // A file no longer set aside stays named until it is recorded so.
        side.keep(9, b"ijkl", &[true, false]).unwrap();
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
        assert!(Side::open(path.clone(), 4, 3).is_err());
        Side::remove(&path).unwrap();
        Side::remove(&path).unwrap();
        assert!(!path.exists());
    }
}
