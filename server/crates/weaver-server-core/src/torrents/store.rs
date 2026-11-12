// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a torrent has, and where: its listed files under its directory, as
// libtorrent keeps them, and the pieces it holds verified in them. A
// verified piece is written to its files once, from the buffer it was
// hashed in, and the same buffer goes on to the tee when the torrent has
// one: no byte is copied or hashed again for it. The files are what the
// torrent seeds from; nothing else writes them.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use bytes::Bytes;

use super::files::Files;
use super::layout::PieceLayout;
use super::side::Side;
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
    // Which files are set aside: a piece never writes them, its share of
    // them goes to the side store, and a share kept there is read from
    // there. Taken to read for a piece's write or a read, and to write only
    // by a change of which files are, so a change waits on every write and
    // read under way and none starts until it is done.
    aside: RwLock<Vec<bool>>,
    side: Mutex<Side>,
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
            aside: RwLock::new(Vec::new()),
            side: Mutex::new(Side::none()),
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

    // The store with its side store at `side`, and the files `ignored`
    // names set aside. A slot of a piece not had, unless a check of every
    // piece is under way, or one no file set aside has a share in, is
    // dropped: what it holds is no piece's the torrent reads from there.
    // The only files written here are those the side store's head names
    // and `ignored` does not: their shares of the pieces kept are copied
    // out to them. Nothing reads or writes the store before it is made, so
    // that copy comes before all else, as a change of levels's does.
    pub(crate) fn with_side(mut self, side: PathBuf, ignored: &[bool]) -> io::Result<Self> {
        let aside: Vec<bool> = self
            .layout
            .files()
            .enumerate()
            .map(|(index, file)| !file.padding && ignored.get(index).copied().unwrap_or(false))
            .collect();
        let mut opened = Side::open(side, self.layout.piece_length(), self.layout.file_count())?;
        let checking = self.have().unchecked.any();
        let mut buffer = Vec::new();
        for piece in opened.pieces() {
            if !(checking || self.has(piece)) {
                opened.drop_piece(piece)?;
                continue;
            }
            // A file set aside when the piece was kept and wanted now, its
            // levels changed while the torrent had no store, takes its
            // share out first, as a change of levels would.
            for span in self.layout.spans(piece) {
                if opened.recorded(span.file) && !aside[span.file] {
                    buffer.resize(span.range.len() as usize, 0);
                    opened.read(piece, span.at, &mut buffer)?;
                    self.files.write(span.file, span.range.start, &buffer)?;
                }
            }
            if !self.layout.spans(piece).iter().any(|span| aside[span.file]) {
                opened.drop_piece(piece)?;
            }
        }
        opened.record(&aside)?;
        *self.aside.get_mut().unwrap_or_else(PoisonError::into_inner) = aside;
        *self.side.get_mut().unwrap_or_else(PoisonError::into_inner) = opened;
        Ok(self)
    }

    fn aside(&self) -> RwLockReadGuard<'_, Vec<bool>> {
        self.aside.read().unwrap_or_else(PoisonError::into_inner)
    }

    // Holds off a change of which files are set aside while held, as a
    // piece's write or a read under way does.
    #[cfg(test)]
    pub(crate) fn hold_reads(&self) -> RwLockReadGuard<'_, Vec<bool>> {
        self.aside()
    }

    fn aside_mut(&self) -> RwLockWriteGuard<'_, Vec<bool>> {
        self.aside.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn side(&self) -> MutexGuard<'_, Side> {
        self.side.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Where its side store is, when it has one.
    pub(crate) fn side_path(&self) -> Option<PathBuf> {
        self.side().path().map(Path::to_path_buf)
    }

    // The pieces its side store keeps.
    #[cfg(test)]
    pub(crate) fn side_pieces(&self) -> Vec<u32> {
        self.side().pieces()
    }

    // Whether setting aside the files `ignored` names, and no other, has a
    // file's bytes to copy out of the side store into the file first.
    pub(crate) fn needs_copy(&self, ignored: &[bool]) -> bool {
        let aside = self.aside();
        let side = self.side();
        side.pieces().into_iter().any(|piece| {
            self.layout.spans(piece).iter().any(|span| {
                aside.get(span.file).copied().unwrap_or(false)
                    && !ignored.get(span.file).copied().unwrap_or(false)
            })
        })
    }

    // The files `ignored` names set aside from now on, and no other. A file
    // no longer set aside first takes its shares out of the side store,
    // written to it where they lie, and a slot no file set aside still has
    // a share in is dropped. Every write and read of the torrent's pieces
    // waits until it is done (libtorrent runs it as a fence job,
    // `mmap_storage::set_file_priority`, mmap_storage.cpp:176-234 at
    // v2.0.15). A copy that fails changes nothing: the bytes stay in the
    // side store, and the files set aside are as they were.
    pub(crate) fn relevel(&self, ignored: &[bool]) -> io::Result<()> {
        let mut aside = self.aside_mut();
        let mut side = self.side();
        let wanted: Vec<bool> = self
            .layout
            .files()
            .enumerate()
            .map(|(index, file)| !file.padding && ignored.get(index).copied().unwrap_or(false))
            .collect();
        let turned_on = |file: usize| aside[file] && !wanted[file];
        let touched: Vec<u32> = side
            .pieces()
            .into_iter()
            .filter(|piece| {
                self.layout
                    .spans(*piece)
                    .iter()
                    .any(|span| turned_on(span.file))
            })
            .collect();
        let mut buffer = Vec::new();
        for piece in &touched {
            for span in self.layout.spans(*piece) {
                if !turned_on(span.file) {
                    continue;
                }
                buffer.resize(span.range.len() as usize, 0);
                side.read(*piece, span.at, &mut buffer)?;
                self.files.write(span.file, span.range.start, &buffer)?;
            }
        }
        if !touched.is_empty() {
            // The files copied out are no longer named in the head; one
            // turned on with nothing to copy stays named, harmless, until
            // the next copy or open.
            side.record(&wanted)?;
        }
        *aside = wanted;
        for piece in touched {
            let shares = self.layout.spans(piece).iter().any(|span| aside[span.file]);
            if !shares {
                // A tag left behind names a slot no file set aside has a
                // share in: the next open drops it.
                let _ = side.drop_piece(piece);
            }
        }
        Ok(())
    }

    // The file's bytes from `offset` into `out`, with which files are set
    // aside: a share of a file set aside that the side store keeps comes
    // from there, any other from the file.
    fn read_in(&self, aside: &[bool], file: usize, offset: u64, out: &mut [u8]) -> io::Result<()> {
        if !aside.get(file).copied().unwrap_or(false) {
            return self.files.read(file, offset, out);
        }
        let side = self.side();
        if side.is_empty() {
            drop(side);
            return self.files.read(file, offset, out);
        }
        let start = self.layout.torrent_offset(file, offset).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("no file {file} in the torrent"),
            )
        })?;
        let piece_length = self.layout.piece_length();
        let mut done = 0;
        while done < out.len() {
            let at = start + done as u64;
            let piece = (at / piece_length) as u32;
            let within = at % piece_length;
            let len = ((piece_length - within) as usize).min(out.len() - done);
            let chunk = &mut out[done..done + len];
            if side.kept(piece) {
                side.read(piece, within, chunk)?;
            } else {
                self.files.read(file, offset + done as u64, chunk)?;
            }
            done += len;
        }
        Ok(())
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

    // Each file's bytes in the pieces it has, into `into`, reusing its room.
    pub(crate) fn file_have_into(&self, into: &mut Vec<u64>) {
        into.clone_from(&self.have().file_have);
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

    // The names of the listed files it has whole.
    // A file set aside with any of its bytes in the side store is not whole
    // on disk.
    pub(crate) fn whole_names(&self) -> Vec<String> {
        let aside = self.aside();
        let kept: Vec<usize> = self
            .side()
            .pieces()
            .into_iter()
            .flat_map(|piece| self.layout.spans(piece))
            .map(|span| span.file)
            .filter(|file| aside.get(*file).copied().unwrap_or(false))
            .collect();
        let have = self.have();
        self.layout
            .files()
            .enumerate()
            .filter(|(index, file)| {
                !file.padding && have.file_have[*index] >= file.length && !kept.contains(index)
            })
            .map(|(_, file)| file.name.clone())
            .collect()
    }

    // The piece's shares the tee waits on go to it from the files: the
    // read a restart, or a seed added over files already there, makes to
    // bring it level with the pieces the torrent has. True when it took any.
    pub(crate) fn replay(&self, piece: u32) -> io::Result<bool> {
        let Some(tee) = &self.tee else {
            return Ok(false);
        };
        tee.replay(piece, |file, offset, out| {
            self.read_in(&self.aside(), file, offset, out)
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

    // libtorrent `storage_utils::verify_resume_data`: the pieces kept are
    // had only while the files hold them. Each file with bytes in a piece
    // had is looked at once, by its size alone, never read: a piece whose
    // share of a file lies past that file's end, or in a file not there, is
    // had no longer, so the session checks or fetches it again. How many
    // pieces that took. A file whose pieces are all still in it costs one
    // look and changes nothing.
    //
    // When not one of those files can be found, nothing is had no longer
    // and none is returned: that is a drive or share not there yet as much
    // as files deleted, and the pieces stay for when it is (qBittorrent
    // keeps a torrent's progress whose resume data was rejected,
    // `TorrentImpl::prepareResumeData`, torrentimpl.cpp:2305-2318). A file
    // gone while another is there is a file lost, as libtorrent finds it.
    //
    // A share of a file set aside that the side store keeps is backed by
    // it, whatever the file holds: a file whose bytes had are all kept
    // there is not looked at, and counts neither as there nor as gone.
    pub(crate) fn drop_unbacked(&self) -> Option<u32> {
        let aside = self.aside();
        let kept = self.side().pieces();
        let mut in_side = vec![0u64; self.layout.file_count()];
        {
            let have = self.have();
            for piece in &kept {
                if !have.pieces.get(*piece as usize).is_some_and(|bit| *bit) {
                    continue;
                }
                for span in self.layout.spans(*piece) {
                    if aside.get(span.file).copied().unwrap_or(false) {
                        in_side[span.file] += span.range.len();
                    }
                }
            }
        }
        let backed = |file: usize, piece: u32| {
            aside.get(file).copied().unwrap_or(false) && kept.binary_search(&piece).is_ok()
        };
        let touched: Vec<(usize, u64)> = self
            .have()
            .file_have
            .iter()
            .enumerate()
            .filter(|(file, had)| **had > in_side[*file])
            // Padding is on no disk, and holds nothing to lose.
            .filter_map(|(file, _)| {
                let found = self.layout.file(file).filter(|found| !found.padding)?;
                Some((file, found.length))
            })
            .collect();
        let held: Vec<(usize, u64, Option<u64>)> = touched
            .into_iter()
            .map(|(file, length)| (file, length, self.files.held(file)))
            .collect();
        if !held.is_empty() && held.iter().all(|(_, _, held)| held.is_none()) {
            return None;
        }
        let mut dropped = 0;
        for (file, length, held) in held {
            let held = held.unwrap_or(0);
            if held >= length {
                continue;
            }
            let mut have = self.have();
            for piece in self.layout.file_pieces(file) {
                let past = !backed(file, piece)
                    && self
                        .layout
                        .share(file, piece)
                        .is_some_and(|share| share.end > held);
                if past && have.set(&self.layout, piece, false) {
                    dropped += 1;
                }
            }
        }
        Some(dropped)
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
}

impl Pieces for TorrentStore {
    // Written to the files, then had, then on to the tee: a piece had is
    // on disk, and a share the tee took is had.
    //
    // A file set aside is never written: when the piece has a share of one,
    // the piece is kept whole in the side store, in the same pass, before
    // it is had (libtorrent `mmap_storage::writev`, mmap_storage.cpp:655-670
    // at v2.0.15).
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        let aside = self.aside();
        let mut kept = false;
        for span in self.layout.spans(piece) {
            if self.layout.file(span.file).is_none_or(|file| file.padding) {
                continue;
            }
            if aside.get(span.file).copied().unwrap_or(false) {
                if !kept {
                    self.side().keep(piece, held, &aside)?;
                    kept = true;
                }
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

    // A piece the side store keeps is dropped from it with the piece.
    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.side().drop_piece(piece)?;
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
        self.read_in(&self.aside(), file, offset, out)
    }

    fn readv(&self, file: usize, mut offset: u64, outs: &mut [&mut [u8]]) -> io::Result<()> {
        let aside = self.aside();
        if !aside.get(file).copied().unwrap_or(false) {
            return self.files.readv(file, offset, outs);
        }
        for out in outs.iter_mut() {
            self.read_in(&aside, file, offset, out)?;
            offset += out.len() as u64;
        }
        Ok(())
    }

    // Only a file it has whole is made its full length, an empty one among
    // them: the others grow as their pieces are written, sparse, as
    // libtorrent leaves them, and a file already there that the torrent has
    // none of yet is left as it is.
    // A file set aside is never made.
    fn grow(&self, file: usize, length: u64) -> io::Result<()> {
        if self.layout.file(file).is_none_or(|file| file.padding)
            || self.aside().get(file).copied().unwrap_or(false)
        {
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
