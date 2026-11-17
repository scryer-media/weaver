// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a torrent has, and where: its listed files under its directory, as
// libtorrent keeps them, and the pieces it holds verified in them. A
// verified piece is written to its files once, from the buffer it was
// hashed in, by the writer of the volume they are on, and the same buffer
// goes on to the tee when the torrent has one: no byte is copied or hashed
// again for it. The files are what the torrent seeds from; nothing else
// writes them.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use bytes::Bytes;

use super::files::{Files, Placed, Stamping};
use super::layout::PieceLayout;
use super::side::Side;
use super::storage::Pieces;
use super::tee::Tee;
use super::writes::{Done, Paid, Paying, Place, VolumeRoom, Writes};
use tokio::sync::Notify;
use weaver_torrent::Checkpoint;
use weaver_torrent::core::lengths::Lengths;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// A copy out of the side store that its writer did not write: nothing of
// the side store changes for it.
fn unwritten() -> io::Error {
    io::Error::other("the copy out of the side store was not written to the torrent's files")
}

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
    // A move of placed files is between the write that set its row aside
    // and the write that ends it (`TorrentStore::mid_move`): the pieces
    // kept meanwhile are to be checked, as that row says.
    mid_move: bool,
    // The files written since their time was last taken for the database,
    // one bit per file: the next checkpoint takes it again.
    stale: BitVec<u8, Msb0>,
    // The pieces kept from the last run in a file whose time changed since
    // then: had no longer, and checked again as the session starts, each
    // had again once the check finds it. Kept for the next run until then.
    recheck: BitVec<u8, Msb0>,
    // Each file's pieces in `recheck`: its time is not taken while any is.
    rechecking: Vec<u32>,
}

// A move of a torrent's placed files is under way until this is dropped
// (`TorrentStore::mid_move`).
pub(crate) struct MidMove<'a>(&'a TorrentStore);

impl Drop for MidMove<'_> {
    fn drop(&mut self) {
        self.0.have().mid_move = false;
    }
}

impl Have {
    fn new(layout: &PieceLayout) -> Self {
        let count = layout.piece_count() as usize;
        Self {
            pieces: BitVec::repeat(false, count),
            unchecked: BitVec::repeat(false, count),
            file_have: vec![0; layout.file_count()],
            count: 0,
            mid_move: false,
            stale: BitVec::repeat(false, layout.file_count()),
            recheck: BitVec::repeat(false, count),
            rechecking: vec![0; layout.file_count()],
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
        // Read back, not written: their times are as the database has them.
        have.stale.fill(false);
        have
    }

    // Had now; true when it was not before. A piece come to be had was
    // written, or found, in its files: their times are taken again.
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
                self.stale.set(span.file, true);
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

    // The piece, kept from the last run, is checked again before it is had:
    // had no longer, and in `recheck` until the check says.
    fn hold_for_check(&mut self, layout: &PieceLayout, piece: u32) -> bool {
        if !self.set(layout, piece, false) {
            return false;
        }
        self.recheck.set(piece as usize, true);
        for span in layout.spans(piece) {
            self.rechecking[span.file] += 1;
        }
        true
    }

    // The check of a piece in `recheck` said, whatever it said: its files'
    // times are taken again, now that they vouch for what they hold.
    fn rechecked(&mut self, layout: &PieceLayout, piece: u32) {
        let Some(mut bit) = self.recheck.get_mut(piece as usize) else {
            return;
        };
        if !*bit {
            return;
        }
        bit.set(false);
        drop(bit);
        for span in layout.spans(piece) {
            self.rechecking[span.file] -= 1;
            self.stale.set(span.file, true);
        }
    }

    // The files were written outside a piece coming to be had: their times
    // are taken again.
    fn wrote(&mut self, files: impl IntoIterator<Item = usize>) {
        for file in files {
            if let Some(mut bit) = self.stale.get_mut(file) {
                bit.set(true);
            }
        }
    }

    // Every file it has bytes of: their times are taken again.
    fn all_written(&mut self) {
        let held: Vec<usize> = (0..self.file_have.len())
            .filter(|file| self.file_have[*file] > 0)
            .collect();
        self.wrote(held);
    }

    // No piece waits on a check of its own: a check of every piece begins.
    fn recheck_none(&mut self) {
        self.recheck.fill(false);
        self.rechecking.fill(0);
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
    // Shared with what follows a piece's writes on its writer.
    have: Arc<Mutex<Have>>,
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
    side: Arc<Mutex<Side>>,
    // Its writes, through the writer of the volume its files are on.
    writes: Writes,
    // Whether its files are moving, and the pieces handed over meanwhile:
    // the thread that hands a piece over never waits for a move.
    moves: Mutex<Moves>,
    // Each file's time as the database last took it, read as the store is
    // made and given up once the files have been looked at
    // (`drop_unbacked`): a file whose time is not that one is checked again.
    stamps: Mutex<HashMap<u32, i64>>,
}

// A piece handed over while its files moved, queued for the files where
// they are once the move ends.
struct Parked {
    piece: u32,
    held: Bytes,
    paid: Option<Paid>,
    done: Done,
}

#[derive(Default)]
struct Moves {
    moving: bool,
    parked: VecDeque<Parked>,
}

impl TorrentStore {
    // The store of a torrent whose files are under `files`, with the pieces
    // `kept` says it had, and the tee its archive files go to, if any. Its
    // pieces are written by `writes`.
    pub(crate) fn new(
        layout: Arc<PieceLayout>,
        files: Files,
        kept: Option<&Kept>,
        tee: Option<Arc<Tee>>,
        writes: Writes,
    ) -> Self {
        let have = Have::kept(&layout, kept);
        let store = Self {
            layout,
            files,
            have: Arc::new(Mutex::new(have)),
            saved: Mutex::new(None),
            tee,
            aside: RwLock::new(Vec::new()),
            side: Arc::new(Mutex::new(Side::none())),
            writes,
            moves: Mutex::default(),
            stamps: Mutex::default(),
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
        // And the times went with them: they are taken again.
        self.have().all_written();
    }

    // Each file's time as the database last took it, for the look at the
    // files that follows (`drop_unbacked`).
    pub(crate) fn set_stamps(&self, stamps: HashMap<u32, i64>) {
        *lock(&self.stamps) = stamps;
    }

    // The files whose times the next checkpoint takes, in the same step as
    // its pieces: each written since its time was last taken, and none
    // with a piece still waiting on its check, whose old time stays until
    // the check says. None when there is none. Their times are taken as
    // taken: a write that lands after marks its file again. None while a
    // move holds the files: a checkpoint never waits out a move, and the
    // times stay due for the one after it.
    pub(crate) fn stamping(&self) -> Option<Stamping> {
        if !self.have().stale.any() {
            return None;
        }
        let dir = self.files.dir_unless_moving()?;
        let files: Vec<u32> = {
            let mut have = self.have();
            let taken: Vec<usize> = have
                .stale
                .iter_ones()
                .filter(|file| have.rechecking[*file] == 0)
                .collect();
            for file in &taken {
                have.stale.set(*file, false);
            }
            // Padding is on no disk: it has no time to take.
            taken
                .into_iter()
                .filter(|file| self.layout.file(*file).is_some_and(|found| !found.padding))
                .map(|file| file as u32)
                .collect()
        };
        let stamping = self.files.stamping(dir, files);
        (!stamping.is_empty()).then_some(stamping)
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
        if opened.reset().is_some() && self.count() > 0 {
            // What the side store held is gone, and with its head which
            // files' shares were there alone: the store vouches for no
            // piece it had, and every piece is checked again.
            self.check_every_piece();
        }
        let checking = self.have().unchecked.any();
        // Dropped once the copies out of them are written.
        let mut copied = Vec::new();
        // The files written: their times are taken again.
        let mut wrote = Vec::new();
        {
            let placed = self.files.placed();
            let barrier = self.writes.queue(|batch| {
                for piece in opened.pieces() {
                    if !(checking || self.has(piece)) {
                        opened.drop_piece(piece)?;
                        continue;
                    }
                    // A file set aside when the piece was kept and wanted
                    // now, its levels changed while the torrent had no
                    // store, takes its share out first, as a change of
                    // levels would.
                    for span in self.layout.spans(piece) {
                        if opened.recorded(span.file) && !aside[span.file] {
                            let mut buffer = vec![0; span.range.len() as usize];
                            opened.read(piece, span.at, &mut buffer)?;
                            let place = placed.place(span.file)?;
                            batch.write(span.file, span.range.start, Bytes::from(buffer), || place);
                            wrote.push(span.file);
                        }
                    }
                    if !self.layout.spans(piece).iter().any(|span| aside[span.file]) {
                        copied.push(piece);
                    }
                }
                io::Result::Ok(batch.barrier())
            })?;
            barrier.wait_aside();
            if barrier.unwritten() {
                return Err(unwritten());
            }
        }
        self.have().wrote(wrote);
        for piece in copied {
            opened.drop_piece(piece)?;
        }
        opened.record(&aside)?;
        *self.aside.get_mut().unwrap_or_else(PoisonError::into_inner) = aside;
        *lock(&self.side) = opened;
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
        lock(&self.side)
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
    // file's bytes to copy out of the side store into the file first, or
    // pieces still to be written to wait for first: one of them may be on
    // its way to the side store for a file no longer set aside. Either
    // waits on a disk.
    pub(crate) fn needs_copy(&self, ignored: &[bool]) -> bool {
        let aside = self.aside();
        let turned_on = |file: usize| {
            aside.get(file).copied().unwrap_or(false)
                && !ignored.get(file).copied().unwrap_or(false)
        };
        if (0..aside.len()).any(turned_on) && self.writes.out() {
            return true;
        }
        let side = self.side();
        side.pieces().into_iter().any(|piece| {
            self.layout
                .spans(piece)
                .iter()
                .any(|span| turned_on(span.file))
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
        let wanted: Vec<bool> = self
            .layout
            .files()
            .enumerate()
            .map(|(index, file)| !file.padding && ignored.get(index).copied().unwrap_or(false))
            .collect();
        let turned_on = |file: usize| aside[file] && !wanted[file];
        if (0..aside.len()).any(turned_on) {
            // A piece handed to its writer before, set aside as the files
            // were then, is kept in the side store first, so a share of a
            // file turned on is copied out with the rest. None is handed
            // over while this holds the files set aside.
            self.writes.settle();
        }
        let placed = self.files.placed();
        let mut side = self.side();
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
        if !touched.is_empty() {
            let barrier = self.writes.queue(|batch| {
                for piece in &touched {
                    for span in self.layout.spans(*piece) {
                        if !turned_on(span.file) {
                            continue;
                        }
                        let mut buffer = vec![0; span.range.len() as usize];
                        side.read(*piece, span.at, &mut buffer)?;
                        let place = placed.place(span.file)?;
                        batch.write(span.file, span.range.start, Bytes::from(buffer), || place);
                    }
                }
                io::Result::Ok(batch.barrier())
            })?;
            barrier.wait_aside();
            if barrier.unwritten() {
                return Err(unwritten());
            }
            // The files written: their times are taken again.
            self.have().wrote(
                touched
                    .iter()
                    .flat_map(|piece| self.layout.spans(*piece))
                    .map(|span| span.file)
                    .filter(|file| turned_on(*file)),
            );
        }
        drop(placed);
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
        lock(&self.have)
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

    // A check of every piece begun: none is had until it finds it. True
    // when any was had.
    //
    // Files a move shut to reads (`Files::relocate`) are read again from
    // here, once no piece is had: this is the one place every piece is
    // forgotten, so whatever begins the check (the move itself, a recheck
    // the operator asks for, the start after an error, a session joined)
    // reads them, and none is left with files it can never check.
    fn check_every_piece(&self) -> bool {
        let had = {
            let mut have = self.have();
            let had = have.pieces.clone();
            have.unchecked.fill(true);
            for piece in had.iter_ones() {
                have.set(&self.layout, piece as u32, false);
            }
            let rechecking = have.recheck.any();
            have.recheck_none();
            had.any() || rechecking
        };
        self.files.let_read();
        had
    }

    // Why the side store found at open was not trusted and was removed,
    // when it was not.
    pub(crate) fn side_reset(&self) -> Option<&'static str> {
        self.side().reset()
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
        let mut bits = have.pieces.as_raw_slice().to_vec();
        // A piece waiting on its check is kept as it was: had, unless the
        // check says it is not.
        if have.recheck.any() {
            for (byte, recheck) in bits.iter_mut().zip(have.recheck.as_raw_slice()) {
                *byte |= *recheck;
            }
        }
        Kept {
            bits,
            checking: have.unchecked.any() || have.mid_move,
        }
    }

    // A move of the torrent's placed files begins: until what is returned
    // is dropped, after the write that ends the move, the pieces kept for
    // the next run are to be checked (`kept`), as the row the move set
    // aside says. A run that ends while files are copied then trusts none
    // of them, whatever checkpoint was written meanwhile. It forgets no
    // piece and marks none unchecked: a move that keeps nothing leaves the
    // pieces vouched for as they were.
    pub(crate) fn mid_move(&self) -> MidMove<'_> {
        self.have().mid_move = true;
        MidMove(self)
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
    //
    // Files still shut to reads have their pieces forgotten here first: a
    // session joined with them holds none as had and checks them all.
    pub(crate) fn initial_have(&self, placed: bool) -> Option<BitBox<u8, Msb0>> {
        if self.files.is_shut() {
            self.check_every_piece();
        }
        let have = self.have();
        if have.unchecked.any() || (placed && have.count == 0 && !have.recheck.any()) {
            return None;
        }
        let mut pieces = have.pieces.clone();
        pieces.set_uninitialized(false);
        Some(pieces.into_boxed_bitslice())
    }

    // `initial_have` as the session's checkpoint, with every block of each
    // piece waiting on its check marked written: the session checks those
    // pieces alone before it starts, and the store has each again that the
    // check finds (libtorrent `torrent::on_resume_data_checked`, which
    // verifies each unfinished piece whose blocks are all there).
    pub(crate) fn initial_checkpoint(&self, placed: bool) -> Option<Checkpoint> {
        let have = self.initial_have(placed)?;
        let mut checkpoint = Checkpoint::pieces(have);
        let recheck = self.have().recheck.clone();
        if !recheck.any() {
            return Some(checkpoint);
        }
        // A layout the engine cannot take is one it refuses whole: the
        // pieces had go on as they were, and those waiting are fetched.
        let piece_length = u32::try_from(self.layout.piece_length()).ok();
        let Some(lengths) = piece_length.and_then(|piece_length| {
            Lengths::new(self.layout.total(), piece_length).ok()
        }) else {
            return Some(checkpoint);
        };
        let chunks = lengths.chunk_bitfield_bytes();
        let mut blocks: BitBox<u8, Msb0> = BitBox::from_boxed_slice(vec![0u8; chunks].into());
        for piece in recheck.iter_ones() {
            if let Some(piece) = lengths.validate_piece_index(piece as u32) {
                blocks[lengths.chunk_range(piece)].fill(true);
            }
        }
        checkpoint.blocks = Some(blocks);
        Some(checkpoint)
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
        // A set, so a torrent of many files and many kept pieces is not
        // searched once per file.
        let kept: std::collections::BTreeSet<usize> = self
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
        let held: Vec<_> = touched
            .into_iter()
            .map(|(file, length)| (file, length, self.files.held_since(file)))
            .collect();
        if !held.is_empty() && held.iter().all(|(_, _, held)| held.is_none()) {
            return None;
        }
        // Looked at: the times read with the pieces are not needed again.
        let stamps = std::mem::take(&mut *lock(&self.stamps));
        let mut dropped = 0;
        let (mut changed, mut rechecked) = (0u32, 0u32);
        for (file, length, held) in held {
            let (held, time) = held.unwrap_or((0, None));
            let mut have = self.have();
            if held < length {
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
            // A file whose time is not the one taken with its pieces was
            // changed since: what it still holds of them is checked again.
            // One written in this run already, or with no usable time now
            // or then, is judged by its size alone.
            let then = stamps.get(&(file as u32)).copied();
            let differs = matches!((then, time), (Some(then), Some(now)) if then != now);
            if !differs || have.stale.get(file).is_some_and(|bit| *bit) {
                continue;
            }
            changed += 1;
            for piece in self.layout.file_pieces(file) {
                if !backed(file, piece) && have.hold_for_check(&self.layout, piece) {
                    rechecked += 1;
                }
            }
        }
        if changed > 0 {
            tracing::info!(
                files = changed,
                pieces = rechecked,
                "a torrent's files changed since it last ran; their pieces are checked again"
            );
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

    // The torrent going away: what it has still queued to be written is
    // given up and out of the way before this returns, its files' handles
    // are let go, and its tee hears nothing more and gives back what it
    // holds of the repair slots.
    pub(crate) async fn forget(&self) {
        drop(self.unpark());
        self.writes.forget().await;
        self.files.release();
        if let Some(tee) = &self.tee {
            tee.forget();
        }
    }

    // `forget` for a test off the async runtime.
    #[cfg(test)]
    pub(crate) fn forget_now(&self) {
        drop(self.unpark());
        if let Some(barrier) = self.writes.abandon() {
            barrier.wait_aside();
        }
        self.files.release();
        if let Some(tee) = &self.tee {
            tee.forget();
        }
    }

    #[cfg(test)]
    pub(crate) fn writes(&self) -> &Writes {
        &self.writes
    }

    // Move the files, as `Files::relocate` does, once every write queued
    // for them where they are has been written: those after go where they
    // went. For a thread that may wait on a disk.
    // Pieces handed over while it lasts are kept, and queued as it ends.
    // `shut` says whether the files come out of it shut to reads
    // (`Files::relocate`).
    pub(crate) fn relocate<T, E>(
        &self,
        relocate: impl FnOnce(&Path) -> Result<(PathBuf, T), E>,
        shut: impl FnOnce(Result<&T, &E>) -> bool,
    ) -> Result<T, E> {
        self.begin_move();
        let moved = self.files.relocate(
            |dir| {
                if !self.writes.drain() {
                    tracing::warn!(dir = %dir.display(), "a torrent's files moved with pieces queued for them unwritten");
                }
                let (to, outcome) = relocate(dir)?;
                self.writes.moved(&to);
                Ok((to, outcome))
            },
            // Files that come out shut are vouched for by nothing saved:
            // the pieces kept for the next run say so (`kept`), and that
            // run checks them all, until a check of every piece has found
            // them. Said while the move still holds the files, so no
            // checkpoint finds them moved and the pieces vouched for.
            |ended| {
                let shut = shut(ended);
                if shut {
                    self.unvouch();
                }
                shut
            },
        );
        // And files shut by a move before this one stay unvouched for.
        if self.files.is_shut() {
            self.unvouch();
        }
        // A move may copy the files, and a copy keeps no time: every file
        // it has bytes of has its time taken again where it is now.
        if moved.is_ok() {
            self.have().all_written();
        }
        self.end_move();
        moved
    }

    // Whether the store vouches for none of its pieces: a check of every
    // piece has yet to find them.
    pub(crate) fn unvouched(&self) -> bool {
        self.have().unchecked.any()
    }

    // The store vouches for none of its pieces from here, until a check of
    // every piece has found them: the pieces kept for the next run say so
    // (`kept`). It forgets none; that is `check_every_piece`.
    pub(crate) fn unvouch(&self) {
        self.have().unchecked.fill(true);
    }

    // The files are under `to` from now on, moved by no one: a torrent held
    // for its files, which writes nothing. Its writes go there when it
    // does.
    pub(crate) fn place(&self, to: PathBuf) {
        self.begin_move();
        self.files
            .relocate(
                |_| {
                    self.writes.placed(&to);
                    Ok::<_, std::convert::Infallible>((to.clone(), ()))
                },
                |_| false,
            )
            .unwrap_or_else(|never| match never {});
        self.have().all_written();
        self.end_move();
    }

    // A move begins: from here no piece handed over waits for the files,
    // it is kept until the move ends. Said before the files are taken, so a
    // piece handed over before it is queued under them first. One torrent's
    // moves never overlap: the service refuses a move while one is under
    // way.
    fn begin_move(&self) {
        lock(&self.moves).moving = true;
    }

    // The move ended, the files where they now are: the pieces kept while
    // it lasted are queued for them, in the order they were handed over,
    // and those handed over meanwhile behind them. On the thread that
    // moved, which may wait on a disk.
    fn end_move(&self) {
        loop {
            let parked = {
                let mut moves = lock(&self.moves);
                if moves.parked.is_empty() {
                    moves.moving = false;
                    return;
                }
                std::mem::take(&mut moves.parked)
            };
            let placed = self.files.placed();
            for mut parked in parked {
                // Counted on the volume the files left: given back there,
                // and counted where they are as its writes fit.
                let paying = match self.writes.pay(parked.paid.take()) {
                    Ok(paying) => paying,
                    Err(elsewhere) => {
                        drop(elsewhere);
                        self.paying_unpaid()
                    }
                };
                self.queue_piece(&placed, paying, parked);
            }
        }
    }

    // The pieces kept for a move that will not be written: given up with
    // the torrent's writes.
    fn unpark(&self) -> VecDeque<Parked> {
        std::mem::take(&mut lock(&self.moves).parked)
    }

    // The piece handed to the writer of its files' volume, paid out of
    // `paid`: its shares of files set aside are kept in the side store
    // instead, once the rest is written, in the same turn on the writer.
    // Then it is had, and then it goes on to the tee, off the writer. `done`
    // hears after all of it, or hears the error of a write that failed, or
    // is dropped when the writes were given up.
    //
    // While the files move, the piece is kept instead, and queued when the
    // move ends. A piece whose room was counted on a volume its files are no
    // longer on is handed back: queued here, it would stand still until the
    // volume they are on has room for it.
    fn hand_over(&self, parked: Parked) -> Option<Parked> {
        let mut moves = lock(&self.moves);
        if moves.moving {
            moves.parked.push_back(parked);
            return None;
        }
        // Not waited for: a move says it begins before it takes the files.
        let placed = self.files.placed();
        drop(moves);
        let mut parked = parked;
        let paying = match self.writes.pay(parked.paid.take()) {
            Ok(paying) => paying,
            Err(elsewhere) => {
                parked.paid = Some(elsewhere);
                return Some(parked);
            }
        };
        self.queue_piece(&placed, paying, parked);
        None
    }

    // Nothing paid: each write is counted as it fits.
    fn paying_unpaid(&self) -> Paying {
        match self.writes.pay(None) {
            Ok(paying) => paying,
            Err(_) => unreachable!("nothing paid is counted nowhere"),
        }
    }

    // The piece's writes queued for the files where `placed` has them,
    // paid out of `paying`.
    fn queue_piece(&self, placed: &Placed<'_>, paying: Paying, parked: Parked) {
        let Parked {
            piece, held, done, ..
        } = parked;
        let aside = self.aside();
        self.writes.queue_paying(paying, move |batch| {
            let mut kept: Option<Vec<bool>> = None;
            for span in self.layout.spans(piece) {
                if self.layout.file(span.file).is_none_or(|file| file.padding) {
                    continue;
                }
                if aside.get(span.file).copied().unwrap_or(false) {
                    if kept.is_none() {
                        kept = Some(aside.clone());
                    }
                    continue;
                }
                let at = span.at as usize;
                let len = span.range.len() as usize;
                placed.count_write(span.file, span.range.start, len);
                batch.write(
                    span.file,
                    span.range.start,
                    held.slice(at..at + len),
                    || {
                        placed.place(span.file).unwrap_or_else(|_| Place {
                            path: PathBuf::new(),
                            within: PathBuf::new(),
                        })
                    },
                );
            }
            let have = Arc::clone(&self.have);
            let side = Arc::clone(&self.side);
            let layout = Arc::clone(&self.layout);
            let tee = self.tee.clone();
            let writes = self.writes.clone();
            batch.then(done, move |done| {
                if let Some(aside) = kept
                    && let Err(error) = lock(&side).keep(piece, &held, &aside)
                {
                    return done(Err(error.into()));
                }
                let whole = {
                    let mut have = lock(&have);
                    have.set(&layout, piece, true);
                    have.count == layout.piece_count()
                };
                // What may stand still runs off the writer.
                match tee {
                    Some(tee) => writes.clone().aside(move || {
                        tee.deliver(piece, &held);
                        if whole {
                            writes.let_go();
                        }
                        done(Ok(()));
                    }),
                    None if whole => writes.clone().aside(move || {
                        writes.let_go();
                        done(Ok(()));
                    }),
                    None => done(Ok(())),
                }
            });
        });
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
    //
    // The writes are the writer's, on its own thread: this waits for them,
    // on a thread that may wait on a disk.
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        let (told, stored) = std::sync::mpsc::sync_channel(1);
        let parked = Parked {
            piece,
            held: held.clone(),
            paid: None,
            done: Box::new(move |written| {
                let _ = told.send(written);
            }),
        };
        // Nothing paid: it never waits for room it holds elsewhere.
        drop(self.hand_over(parked));
        stored.recv().unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "the writes of piece {piece} were given up before it was written"
            ))
        })
    }

    // The same, the piece handed over without waiting: one that would
    // wait for its volume's room is handed over off this thread.
    fn store(self: Arc<Self>, piece: u32, held: Bytes, paid: Option<Paid>, done: Done) {
        let parked = Parked {
            piece,
            held,
            paid,
            done,
        };
        if let Some(mut parked) = self.hand_over(parked) {
            // Counted where its files were: given back there, and counted
            // where they are as its writes fit, off this thread.
            parked.paid = None;
            let store = Arc::clone(&self);
            self.writes.aside(move || drop(store.hand_over(parked)));
        }
    }

    fn reserve(&self, bytes: u64, room: &Arc<Notify>) -> VolumeRoom {
        self.writes.reserve(bytes, room)
    }

    fn stop_writes(&self) {
        drop(self.unpark());
        drop(self.writes.abandon());
    }

    // A piece the side store keeps is dropped from it with the piece.
    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.side().drop_piece(piece)?;
        let mut have = self.have();
        if let Some(mut bit) = have.unchecked.get_mut(piece as usize) {
            bit.set(false);
        }
        have.rechecked(&self.layout, piece);
        Ok(have.set(&self.layout, piece, false))
    }

    fn checking(&self) -> anyhow::Result<bool> {
        Ok(self.check_every_piece())
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
            have.rechecked(&self.layout, piece);
            have.set(&self.layout, piece, true)
        };
        self.replay(piece)?;
        Ok(found)
    }

    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.read_in(&self.aside(), file, offset, out)
    }

    fn will_read(&self, file: usize, offset: u64, len: u64) {
        self.files.will_read(file, offset, len);
    }

    fn read_turns(&self) -> weaver_torrent::storage::ReadTurns {
        self.writes.read_turns()
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
        let place = self.files.placed().place(file)?;
        let grown = self.writes.grow(place, length);
        if grown.is_ok() {
            // Made its length: its time is taken again.
            self.have().wrote([file]);
        }
        // A torrent with every piece holds its volume's writer no longer.
        if self.have().count == self.layout.piece_count() {
            self.writes.let_go();
        }
        grown
    }
}
