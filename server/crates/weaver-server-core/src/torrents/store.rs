// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a torrent has, and where: its listed files under its directory, as
// libtorrent keeps them, and the pieces it holds verified in them. A
// verified piece is written to its files once, from the buffer it was
// hashed in, and the same buffer goes on to the tee when the torrent has
// one: no byte is copied or hashed again for it. The files are what the
// torrent seeds from; nothing else writes them.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use bytes::Bytes;

use super::files::Files;
use super::layout::PieceLayout;
use super::storage::Pieces;
use super::tee::Tee;

// The pieces as they are kept between runs: one bit per piece had, most
// significant first, and whether a check of every piece had yet to find
// some, in which case the next run checks every piece again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Kept {
    pub(crate) bits: Vec<u8>,
    pub(crate) checking: bool,
}

impl Kept {
    // How many pieces it has, and each file's bytes in them.
    pub(crate) fn had(&self, layout: &PieceLayout) -> (u32, Vec<u64>) {
        let have = Have::kept(layout, Some(self));
        (have.count, have.file_have)
    }
}

struct Have {
    pieces: BitVec<u8, Msb0>,
    // The pieces a check of every piece has neither found nor failed yet:
    // every piece when it begins, none once it is over. None of them is had
    // until it finds it.
    unchecked: BitVec<u8, Msb0>,
    // Each file's bytes in the pieces had.
    file_have: Vec<u64>,
    count: u32,
}

impl Have {
    fn new(layout: &PieceLayout) -> Self {
        let count = layout.piece_count() as usize;
        Self {
            pieces: BitVec::repeat(false, count),
            unchecked: BitVec::repeat(false, count),
            file_have: vec![0; layout.file_count()],
            count: 0,
        }
    }

    fn kept(layout: &PieceLayout, kept: Option<&Kept>) -> Self {
        let mut have = Self::new(layout);
        if let Some(kept) = kept {
            let bits = BitVec::<u8, Msb0>::from_slice(&kept.bits);
            let count = have.pieces.len();
            for piece in bits.iter_ones().take_while(|piece| *piece < count) {
                have.set(layout, piece as u32, true);
            }
            // A check that was not over checks every piece again.
            if kept.checking {
                have.unchecked.fill(true);
            }
        }
        have
    }

    // Had now; true when it was not before.
    fn set(&mut self, layout: &PieceLayout, piece: u32, had: bool) -> bool {
        let Some(mut bit) = self.pieces.get_mut(piece as usize) else {
            return false;
        };
        if *bit == had {
            return false;
        }
        bit.set(had);
        drop(bit);
        for span in layout.spans(piece) {
            let have = &mut self.file_have[span.file];
            if had {
                *have += span.range.len();
            } else {
                *have -= span.range.len();
            }
        }
        if had {
            self.count += 1;
        } else {
            self.count -= 1;
        }
        true
    }
}

// What a replay of the pieces had gave the tee: the pieces it took, and
// the pieces whose files did not read, had no longer, with the first error.
#[derive(Debug, Default)]
pub(crate) struct Replayed {
    pub(crate) taken: u32,
    pub(crate) dropped: u32,
    pub(crate) unread: Option<io::Error>,
}

pub(crate) struct TorrentStore {
    layout: Arc<PieceLayout>,
    files: Files,
    have: Mutex<Have>,
    // The pieces as the database last took them, or as they were read from
    // it: a checkpoint writes them again only once they differ.
    saved: Mutex<Option<Kept>>,
    tee: Option<Arc<Tee>>,
}

impl TorrentStore {
    // The store of a torrent whose files are under `files`, with the pieces
    // `kept` says it had, and the tee its archive files go to, if any.
    pub(crate) fn new(
        layout: Arc<PieceLayout>,
        files: Files,
        kept: Option<&Kept>,
        tee: Option<Arc<Tee>>,
    ) -> Self {
        let have = Have::kept(&layout, kept);
        let store = Self {
            layout,
            files,
            have: Mutex::new(have),
            saved: Mutex::new(None),
            tee,
        };
        *store.saved() = Some(store.kept());
        store
    }

    fn saved(&self) -> MutexGuard<'_, Option<Kept>> {
        self.saved.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // The pieces to write when they changed since last written, taken as
    // written; none when the database has them already.
    pub(crate) fn unsaved(&self) -> Option<Kept> {
        let kept = self.kept();
        let mut saved = self.saved();
        if saved.as_ref() == Some(&kept) {
            return None;
        }
        *saved = Some(kept.clone());
        Some(kept)
    }

    // A write of the pieces failed: what the database holds is unknown, so
    // the next checkpoint writes them.
    pub(crate) fn lost_saved(&self) {
        *self.saved() = None;
    }

    fn have(&self) -> MutexGuard<'_, Have> {
        self.have.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn layout(&self) -> &Arc<PieceLayout> {
        &self.layout
    }

    pub(crate) fn files(&self) -> &Files {
        &self.files
    }

    pub(crate) fn tee(&self) -> Option<&Arc<Tee>> {
        self.tee.as_ref()
    }

    pub(crate) fn has(&self, piece: u32) -> bool {
        self.have()
            .pieces
            .get(piece as usize)
            .is_some_and(|bit| *bit)
    }

    pub(crate) fn count(&self) -> u32 {
        self.have().count
    }

    pub(crate) fn file_have(&self) -> Vec<u64> {
        self.have().file_have.clone()
    }

    // The pieces to keep for the next run.
    pub(crate) fn kept(&self) -> Kept {
        let have = self.have();
        Kept {
            bits: have.pieces.as_raw_slice().to_vec(),
            checking: have.unchecked.any(),
        }
    }

    // The pieces the session starts from, so it does not check the files:
    // one bit per piece, the spare bits of the last byte clear, as BEP 3
    // sends them. None while a check of every piece has not found every
    // piece had: the store vouches for nothing then, and the session checks
    // every piece again (libtorrent `torrent::write_resume_data` saves no
    // piece past those checked). None too while it has no piece of files
    // that were `placed` where its client asked: they may be there already,
    // and the check finds them (libtorrent checks the files of a torrent
    // added with no resume data).
    pub(crate) fn initial_have(&self, placed: bool) -> Option<BitBox<u8, Msb0>> {
        let have = self.have();
        if have.unchecked.any() || (placed && have.count == 0) {
            return None;
        }
        let mut pieces = have.pieces.clone();
        pieces.set_uninitialized(false);
        Some(pieces.into_boxed_bitslice())
    }

    // Whether every file `wanted` names is had whole.
    pub(crate) fn has_files(&self, wanted: impl Fn(usize) -> bool) -> bool {
        let have = self.have();
        self.layout
            .files()
            .enumerate()
            .filter(|(index, file)| !file.padding && wanted(*index))
            .all(|(index, file)| have.file_have[index] >= file.length)
    }

    // The piece's shares the tee waits on go to it from the files: the
    // read a restart, or a seed added over files already there, makes to
    // bring it level with the pieces the torrent has. True when it took any.
    pub(crate) fn replay(&self, piece: u32) -> io::Result<bool> {
        let Some(tee) = &self.tee else {
            return Ok(false);
        };
        tee.replay(piece, |file, offset, out| {
            self.files.read(file, offset, out)
        })
    }

    // Each piece had that the tee waits on, replayed to it once. A piece a
    // check of every piece has yet to find is not: the check replays it
    // when it does. A piece whose files do not read, a file gone or a drive
    // not there yet, is had no longer, so the session checks or fetches it
    // again, as libtorrent does a piece its files lost.
    pub(crate) fn replay_had(&self) -> Replayed {
        let had: Vec<u32> = {
            let have = self.have();
            let mut vouched = have.pieces.clone();
            vouched &= !have.unchecked.clone();
            vouched.iter_ones().map(|piece| piece as u32).collect()
        };
        let mut replayed = Replayed::default();
        for piece in had {
            match self.replay(piece) {
                Ok(true) => replayed.taken += 1,
                Ok(false) => {}
                Err(error) => {
                    self.have().set(&self.layout, piece, false);
                    replayed.dropped += 1;
                    replayed.unread.get_or_insert(error);
                }
            }
        }
        replayed
    }

    // A piece found where the files were placed, had from there, under
    // `keep` (a seed-mode session's `while_seeding`), which reads nothing.
    // The tee is not given it: nothing has checked it yet, and the graph
    // takes only bytes a piece check passed. It takes the piece once its
    // seed-mode check passes, or from the check of every piece that
    // follows one failing. None when `keep` did not take it.
    pub(crate) fn adopt(
        &self,
        piece: u32,
        keep: impl FnOnce(&mut dyn FnMut() -> anyhow::Result<()>) -> Option<anyhow::Result<()>>,
    ) -> Option<anyhow::Result<()>> {
        if self.has(piece) {
            return Some(Ok(()));
        }
        keep(&mut || {
            self.have().set(&self.layout, piece, true);
            Ok(())
        })
    }

    // Whether the tee waits on any share of the piece.
    pub(crate) fn tee_wants(&self, piece: u32) -> bool {
        self.tee.as_ref().is_some_and(|tee| tee.wants(piece))
    }

    // The torrent going away: its files' handles are let go, and its tee
    // hears nothing more and gives back what it holds of the repair slots.
    pub(crate) fn forget(&self) {
        self.files.release();
        if let Some(tee) = &self.tee {
            tee.forget();
        }
    }

    // The paths of the listed files, as they are now.
    pub(crate) fn paths(&self) -> Vec<PathBuf> {
        (0..self.layout.file_count())
            .filter_map(|file| self.files.path(file))
            .collect()
    }
}

impl Pieces for TorrentStore {
    // Written to the files, then had, then on to the tee: a piece had is
    // on disk, and a share the tee took is had.
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        for span in self.layout.spans(piece) {
            if self.layout.file(span.file).is_none_or(|file| file.padding) {
                continue;
            }
            let at = span.at as usize;
            self.files.write(
                span.file,
                span.range.start,
                &held[at..at + span.range.len() as usize],
            )?;
        }
        self.have().set(&self.layout, piece, true);
        if let Some(tee) = &self.tee {
            tee.deliver(piece, held);
        }
        Ok(())
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        let mut have = self.have();
        if let Some(mut bit) = have.unchecked.get_mut(piece as usize) {
            bit.set(false);
        }
        Ok(have.set(&self.layout, piece, false))
    }

    fn checking(&self) -> anyhow::Result<bool> {
        let mut have = self.have();
        let had = have.pieces.clone();
        have.unchecked.fill(true);
        for piece in had.iter_ones() {
            have.set(&self.layout, piece as u32, false);
        }
        Ok(had.any())
    }

    // A check of the files reads a piece the tee waits on whole, and hands
    // it here as it hashed it: the tee takes it from that buffer, so the
    // files are not read for it again. The piece is had when the check
    // says `found`, after; a tee that took a piece the torrent then does
    // not have takes no second copy of it.
    fn wants_hashed(&self, piece: u32) -> bool {
        self.tee.as_ref().is_some_and(|tee| tee.wants(piece))
    }

    fn hashed(&self, piece: u32, held: &Bytes) {
        if let Some(tee) = &self.tee {
            tee.deliver(piece, held);
        }
    }

    // Had again, and on to the tee from the files if it still waits on any
    // of it: a check that found files placed before the torrent was added
    // brings it what they hold.
    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        let found = {
            let mut have = self.have();
            if let Some(mut bit) = have.unchecked.get_mut(piece as usize) {
                bit.set(false);
            }
            have.set(&self.layout, piece, true)
        };
        self.replay(piece)?;
        Ok(found)
    }

    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.files.read(file, offset, out)
    }

    fn readv(&self, file: usize, offset: u64, outs: &mut [&mut [u8]]) -> io::Result<()> {
        self.files.readv(file, offset, outs)
    }

    // Only a file it has whole is made its full length, an empty one among
    // them: the others grow as their pieces are written, sparse, as
    // libtorrent leaves them, and a file already there that the torrent has
    // none of yet is left as it is.
    fn grow(&self, file: usize, length: u64) -> io::Result<()> {
        if self.layout.file(file).is_none_or(|file| file.padding) {
            return Ok(());
        }
        let whole = self
            .have()
            .file_have
            .get(file)
            .is_some_and(|have| *have >= length);
        if !whole {
            return Ok(());
        }
        self.files.grow(file, length)
    }
}
