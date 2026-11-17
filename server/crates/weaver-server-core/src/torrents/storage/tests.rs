// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Notify, watch};
use weaver_graph::NodeId;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant, HoldsNamespace};
use weaver_torrent::core::lengths::{Lengths, ValidPieceIndex};
use weaver_torrent::spawn_utils::BlockingSpawner;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions, create_torrent, torrent_from_bytes,
};

use super::{PieceStorage, PieceStorageFactory, Pieces};
use crate::jobs::JobId;
use crate::torrents::files::{self, Files};
use crate::torrents::intake::Holds;
use crate::torrents::layout::{PieceFile, PieceLayout, layout_of};
use crate::torrents::model::Adopted;
use crate::torrents::relocate::Existing;
use crate::torrents::service::{Listed, delete_written, deletion, move_listed};
use crate::torrents::store::{Kept, TorrentStore};
use weaver_torrent::storage::{
    BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
};

#[cfg(unix)]
mod bench;

fn accountant(resident_limit: u64) -> Arc<HoldsAccountant> {
    Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }))
}

const NAMESPACE: HoldsNamespace = HoldsNamespace {
    scope: 1,
    node: NodeId::ROOT,
};

fn holds(accountant: &Arc<HoldsAccountant>) -> Holds {
    Holds {
        accountant: Arc::clone(accountant),
        namespace: NAMESPACE,
    }
}

fn unbounded() -> Holds {
    holds(&accountant(u64::MAX))
}

fn file(name: &str, length: u64) -> PieceFile {
    PieceFile {
        name: name.to_owned(),
        length,
        padding: false,
    }
}

// Bytes no format takes for its own.
fn noise(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|at| ((at as u32).wrapping_mul(2_654_435_761) >> 13) as u8 ^ seed | 0x80)
        .collect()
}

// The store of a torrent of plain files under `dir`, with what it kept.
fn store_in(layout: &Arc<PieceLayout>, dir: &Path, kept: Option<&Kept>) -> Arc<TorrentStore> {
    let files = Files::new(layout, dir.to_path_buf()).unwrap();
    Arc::new(TorrentStore::new(
        Arc::clone(layout),
        files,
        kept,
        None,
        crate::torrents::writes::Writes::for_test(),
    ))
}

struct Shim {
    store: Arc<TorrentStore>,
    storage: PieceStorage,
    layout: Arc<PieceLayout>,
    dir: tempfile::TempDir,
    room: Arc<Notify>,
}

fn shim(piece_length: u64, files: Vec<PieceFile>) -> Shim {
    shim_in(tempfile::tempdir().unwrap(), piece_length, files)
}

fn shim_in(dir: tempfile::TempDir, piece_length: u64, files: Vec<PieceFile>) -> Shim {
    let layout = Arc::new(PieceLayout::new(piece_length, files).unwrap());
    let store = store_in(&layout, dir.path(), None);
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&store) as Arc<dyn Pieces>,
        unbounded(),
    );
    Shim {
        store,
        storage,
        layout,
        dir,
        room: Arc::default(),
    }
}

impl Shim {
    fn index(&self, piece: u32) -> ValidPieceIndex {
        Lengths::new(
            self.layout.files().map(|file| file.length).sum(),
            self.layout.piece_length() as u32,
        )
        .unwrap()
        .try_validate_piece_index(piece)
        .unwrap()
    }

    // The piece's bytes written as the session writes them: granted room,
    // then file by file.
    fn write(&self, piece: u32, torrent: &[u8]) {
        assert!(self.storage.reserve_piece(self.index(piece), &self.room));
        let range = self.layout.piece_range(piece);
        for span in self.layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            self.storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &torrent[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
    }

    // The piece read back as the session reads it to check or to seed it.
    fn read(&self, piece: u32) -> Vec<u8> {
        let mut out = Vec::new();
        for span in self.layout.spans(piece) {
            let mut buf = vec![0xee; span.range.len() as usize];
            self.storage
                .pread_exact(span.file, span.range.start, &mut buf)
                .unwrap();
            out.extend(buf);
        }
        out
    }

    fn complete(&self, piece: u32, torrent: &[u8]) {
        self.write(piece, torrent);
        self.storage.on_piece_completed(self.index(piece)).unwrap();
    }
}

// A torrent of plain files under its own directory, the files `ignored`
// names set aside in its side store, which is kept apart from them under
// `work`.
fn shim_aside(
    piece_length: u64,
    files: Vec<PieceFile>,
    ignored: &[bool],
    work: &Path,
    kept: Option<&Kept>,
    dir: tempfile::TempDir,
) -> Shim {
    let layout = Arc::new(PieceLayout::new(piece_length, files).unwrap());
    let store = Arc::new(
        TorrentStore::new(
            Arc::clone(&layout),
            Files::new(&layout, dir.path().to_path_buf()).unwrap(),
            kept,
            None,
            crate::torrents::writes::Writes::for_test(),
        )
        .with_side(work.join("torrent-1.side"), ignored)
        .unwrap(),
    );
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&store) as Arc<dyn Pieces>,
        unbounded(),
    );
    Shim {
        store,
        storage,
        layout,
        dir,
        room: Arc::default(),
    }
}

// File a (wanted) is 1536 bytes, file b (not downloaded) 2048 after it,
// pieces of 1024: piece 1 is a's last 512 bytes and b's first 512.
fn wanted_then_ignored(work: &Path, dir: tempfile::TempDir, kept: Option<&Kept>) -> Shim {
    shim_aside(
        1024,
        vec![file("a.bin", 1536), file("sub/b.bin", 2048)],
        &[false, true],
        work,
        kept,
        dir,
    )
}

fn three_files() -> (Shim, Vec<u8>) {
    let shim = shim(
        1024,
        vec![file("a.bin", 1000), file("b.bin", 2500), file("c.bin", 700)],
    );
    let torrent = noise(4200, 3);
    (shim, torrent)
}

#[test]
fn a_piece_in_flight_is_read_back_from_memory() {
    let (shim, torrent) = three_files();
    shim.write(0, &torrent);
    // Before it verifies the piece is the session's alone: the store has
    // none of it, and a read of it comes from the buffer.
    assert_eq!(shim.store.count(), 0);
    assert!(!shim.dir.path().join("a.bin").exists());
    assert_eq!(shim.read(0), torrent[..1024]);
    // A piece with nothing written is on no disk.
    let mut buf = [0; 10];
    assert!(shim.storage.pread_exact(1, 100, &mut buf).is_err());
    // A read across two pieces is read from the files, and piece 1 is not
    // there.
    assert!(shim.storage.pread_exact(1, 20, &mut buf).is_err());
    // Nor does a write cross a piece.
    assert!(shim.storage.pwrite_all(1, 20, &[0; 10]).is_err());
    // An empty read past the end of a file is nothing to do.
    shim.storage.pread_exact(0, 1000, &mut []).unwrap();
}

// A run of a file's bytes read in one go comes from disk, and a piece of it
// still in flight from memory: either way each buffer holds its bytes.
#[test]
fn a_run_read_in_one_go_takes_a_held_piece_from_memory() {
    let (shim, torrent) = three_files();
    for piece in 0..4 {
        if piece == 2 {
            shim.write(piece, &torrent);
        } else {
            shim.complete(piece, &torrent);
        }
    }
    // File b is torrent bytes 1000..3500: pieces 0 to 3.
    let run = |at: u64, lens: &[usize]| {
        let mut read: Vec<Vec<u8>> = lens.iter().map(|len| vec![0xee; *len]).collect();
        let mut bufs: Vec<&mut [u8]> = read.iter_mut().map(Vec::as_mut_slice).collect();
        shim.storage.preadv_exact(1, at, &mut bufs).unwrap();
        read.concat()
    };
    // Pieces 0 and 1, on disk.
    assert_eq!(run(0, &[24, 1000, 24]), torrent[1000..2048]);
    // Pieces 1 to 3, piece 2 in memory.
    assert_eq!(run(24, &[1024, 1024, 428]), torrent[1024..3500]);
    // Past the end of the file is no read.
    let mut over = [0; 600];
    let mut bufs: Vec<&mut [u8]> = vec![&mut over];
    assert!(shim.storage.preadv_exact(1, 2000, &mut bufs).is_err());
}

// libtorrent `disk_io_thread::async_write`: a verified piece is written to
// the torrent's listed files under its directory, and is had.
#[test]
fn a_verified_piece_is_written_to_the_torrents_files() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    assert_eq!(shim.store.count(), 5);
    assert_eq!(shim.store.file_have(), [1000, 2500, 700]);
    let dir = shim.dir.path();
    assert_eq!(std::fs::read(dir.join("a.bin")).unwrap(), torrent[..1000]);
    assert_eq!(
        std::fs::read(dir.join("b.bin")).unwrap(),
        torrent[1000..3500]
    );
    assert_eq!(std::fs::read(dir.join("c.bin")).unwrap(), torrent[3500..]);
    // A piece with nothing written did not verify.
    assert!(shim.storage.on_piece_completed(shim.index(0)).is_err());
}

// The bitfield the session starts from is the pieces the store kept, in
// the wire's bit order.
#[test]
fn a_kept_store_gives_the_session_the_pieces_it_has() {
    let (shim, torrent) = three_files();
    for piece in [0, 1, 2, 4] {
        shim.complete(piece, &torrent);
    }
    let kept = shim.store.kept();
    assert!(!kept.checking);
    let again = store_in(&shim.layout, shim.dir.path(), Some(&kept));
    let have = again.initial_have(false).unwrap();
    assert_eq!(have.len(), 5);
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 4]);
    // Piece 0 is the high bit of the first byte.
    assert_eq!(have.as_raw_slice()[0], 0b1110_1000);
    assert_eq!(again.file_have(), shim.store.file_have());
    // A store with none of files that were placed vouches for nothing: the
    // session checks them.
    let placed = store_in(&shim.layout, shim.dir.path(), None);
    assert!(placed.initial_have(true).is_none());
    assert_eq!(placed.initial_have(false).unwrap().count_ones(), 0);
}

// libtorrent `storage_utils::verify_resume_data`: the pieces kept are had
// at the next start only while their files hold them. A file gone, a file
// cut short and a folder in a file's place each lose the pieces whose bytes
// they no longer hold; a file still its full length, or still past its last
// piece had, keeps them.
#[test]
fn a_kept_store_has_no_piece_its_files_lost() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    let kept = shim.store.kept();
    let dir = shim.dir.path();
    shim.store.forget_now();

    // Nothing changed: every piece stays, and there is nothing to save.
    let again = store_in(&shim.layout, dir, Some(&kept));
    assert_eq!(again.drop_unbacked(), Some(0));
    assert_eq!(again.count(), 5);
    assert!(again.unsaved().is_none());
    again.forget_now();

    // b.bin is torrent bytes 1000..3500, pieces 0 to 3. Cut to 2499 bytes,
    // one short of its length, it loses its last piece only; c.bin, in
    // pieces 3 and 4, is gone.
    let b = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("b.bin"))
        .unwrap();
    b.set_len(2499).unwrap();
    drop(b);
    std::fs::remove_file(dir.join("c.bin")).unwrap();
    let again = store_in(&shim.layout, dir, Some(&kept));
    assert_eq!(again.drop_unbacked(), Some(2));
    let have = again.initial_have(false).unwrap();
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(again.file_have(), [1000, 2048 - 1000 + 1024, 0]);
    // The pieces it lost are saved as lost.
    assert!(again.unsaved().is_some());
    again.forget_now();

    // Cut to 1048 bytes, b.bin still holds pieces 0 and 1 whole, and loses
    // piece 2, whose share of it goes past that.
    let b = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("b.bin"))
        .unwrap();
    b.set_len(1048).unwrap();
    drop(b);
    let again = store_in(&shim.layout, dir, Some(&kept));
    assert_eq!(again.drop_unbacked(), Some(3));
    let have = again.initial_have(false).unwrap();
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 1]);
    again.forget_now();

    // A folder where a.bin was holds none of its bytes.
    std::fs::remove_file(dir.join("a.bin")).unwrap();
    std::fs::create_dir_all(dir.join("a.bin/inner")).unwrap();
    let again = store_in(&shim.layout, dir, Some(&kept));
    assert_eq!(again.drop_unbacked(), Some(4));
    let have = again.initial_have(false).unwrap();
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![1]);
}

// Padding is on no disk: a torrent whose files are all there keeps every
// piece, those that end in padding among them.
#[test]
fn a_kept_store_loses_nothing_to_its_padding() {
    let padding = PieceFile {
        name: ".pad/24".to_owned(),
        length: 24,
        padding: true,
    };
    let shim = shim(1024, vec![file("a.bin", 1000), padding, file("b.bin", 500)]);
    let mut torrent = noise(1524, 7);
    torrent[1000..1024].fill(0);
    for piece in 0..2 {
        shim.complete(piece, &torrent);
    }
    let kept = shim.store.kept();
    shim.store.forget_now();
    let again = store_in(&shim.layout, shim.dir.path(), Some(&kept));
    assert_eq!(again.drop_unbacked(), Some(0));
    assert_eq!(again.count(), 2);
}

// A store that kept no piece looks at no file.
#[test]
fn a_store_with_no_piece_looks_at_no_file() {
    let (shim, _) = three_files();
    assert_eq!(shim.store.drop_unbacked(), Some(0));
    assert_eq!(shim.store.count(), 0);
    assert!(!shim.dir.path().join("a.bin").exists());
}

// A start that finds none of the files with a piece had, as when the drive
// or share they are on is not there yet, keeps every piece and has nothing
// to save; so does one whose directory is gone. One file gone while another
// is there loses that file's pieces only.
#[test]
fn a_kept_store_keeps_its_pieces_while_none_of_its_files_is_there() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    let kept = shim.store.kept();
    let dir = shim.dir.path();
    shim.store.forget_now();
    let aside = tempfile::tempdir().unwrap();
    for name in ["a.bin", "b.bin", "c.bin"] {
        std::fs::rename(dir.join(name), aside.path().join(name)).unwrap();
    }

    let again = store_in(&shim.layout, dir, Some(&kept));
    assert_eq!(again.drop_unbacked(), None);
    assert_eq!(again.count(), 5);
    assert!(again.unsaved().is_none());
    again.forget_now();

    let gone = dir.join("not-mounted");
    let again = store_in(&shim.layout, &gone, Some(&kept));
    assert_eq!(again.drop_unbacked(), None);
    assert_eq!(again.count(), 5);
    assert!(again.unsaved().is_none());
    assert!(!gone.exists());
    again.forget_now();

    // Back again, with a.bin, all of piece 0 and none of the others, still
    // away: only piece 0 is lost.
    for name in ["b.bin", "c.bin"] {
        std::fs::rename(aside.path().join(name), dir.join(name)).unwrap();
    }
    let again = store_in(&shim.layout, dir, Some(&kept));
    assert_eq!(again.drop_unbacked(), Some(1));
    let have = again.initial_have(false).unwrap();
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    assert!(again.unsaved().is_some());
}

// The look at the files costs one size per file with a piece had, and none
// for a torrent that kept no piece, at 1, 100 and 5,000 files.
#[test]
fn a_kept_store_looks_at_each_file_once() {
    use crate::torrents::files::LOOKS;
    for count in [1usize, 100, 5000] {
        let files: Vec<PieceFile> = (0..count)
            .map(|at| file(&format!("f/{at}.bin"), 64))
            .collect();
        let layout = Arc::new(PieceLayout::new(1024, files).unwrap());
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("f")).unwrap();
        for at in 0..count {
            std::fs::write(dir.path().join(format!("f/{at}.bin")), [7u8; 64]).unwrap();
        }
        let pieces = layout.piece_count() as usize;
        let kept = Kept {
            bits: vec![0xff; pieces.div_ceil(8)],
            checking: false,
        };

        let store = store_in(&layout, dir.path(), Some(&kept));
        assert_eq!(store.count() as usize, pieces);
        LOOKS.with(|looks| looks.set(0));
        assert_eq!(store.drop_unbacked(), Some(0));
        assert_eq!(LOOKS.with(std::cell::Cell::get), count as u64);
        store.forget_now();

        let store = store_in(&layout, dir.path(), None);
        LOOKS.with(|looks| looks.set(0));
        assert_eq!(store.drop_unbacked(), Some(0));
        assert_eq!(LOOKS.with(std::cell::Cell::get), 0);
    }
}

// libtorrent `torrent::force_recheck`: once a check of every piece began, a
// run stopped before it gave every piece its verdict has only what it found,
// and checks every piece again.
#[test]
fn a_check_stopped_part_way_is_kept_to_check_again() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    shim.storage.on_check_everything().unwrap();
    assert_eq!(shim.store.count(), 0);
    shim.storage.on_piece_checked(shim.index(0)).unwrap();
    shim.storage.on_piece_hash_failed(shim.index(1)).unwrap();
    assert_eq!(shim.store.count(), 1);
    let kept = shim.store.kept();
    assert!(kept.checking);
    let again = store_in(&shim.layout, shim.dir.path(), Some(&kept));
    assert!(again.initial_have(false).is_none());
    // It has what the check found, and checks every piece again.
    assert_eq!(again.count(), 1);
    assert!(again.has(0));
    for piece in 2..5 {
        shim.storage.on_piece_checked(shim.index(piece)).unwrap();
    }
    let kept = shim.store.kept();
    assert!(!kept.checking);
    let have = store_in(&shim.layout, shim.dir.path(), Some(&kept))
        .initial_have(false)
        .unwrap();
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 2, 3, 4]);
}

#[test]
fn a_verified_piece_seeds_from_the_files_it_was_written_to() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    for piece in 0..5 {
        let range = shim.layout.piece_range(piece);
        assert_eq!(
            shim.read(piece),
            torrent[range.start as usize..range.end as usize],
            "piece {piece}"
        );
    }
}

// libtorrent `torrent::on_piece_hashed` has only the pieces that pass: a
// piece that then fails its hash is had no longer, reads as the file on
// disk holds it, as libtorrent's storage reads whatever the bitfield says,
// and fetched again is had again.
#[test]
fn a_failed_piece_is_had_no_longer_until_fetched_again() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    shim.storage.on_piece_hash_failed(shim.index(2)).unwrap();
    assert!(!shim.store.has(2));
    assert_eq!(shim.store.count(), 4);
    assert_eq!(shim.store.file_have(), [1000, 2500 - 1024, 700]);
    let range = shim.layout.piece_range(2);
    assert_eq!(
        shim.read(2),
        torrent[range.start as usize..range.end as usize]
    );
    shim.complete(2, &torrent);
    assert_eq!(shim.store.count(), 5);
}

// libtorrent `storage_utils::delete_files`: deleting a torrent's files
// removes them, here with what unpacking put beside them, and then each
// directory that leaves empty. A file the operator put beside them stays,
// and so does its directory until it is empty.
#[test]
fn deleting_a_torrents_files_removes_them_and_its_output_and_nothing_else() {
    let shim = shim(1024, vec![file("sub/a.bin", 1000), file("b.bin", 500)]);
    let torrent = noise(1500, 4);
    for piece in 0..2 {
        shim.complete(piece, &torrent);
    }
    let dir = shim.dir.path().to_path_buf();
    std::fs::create_dir_all(dir.join("out")).unwrap();
    std::fs::write(dir.join("out/member.txt"), b"unpacked").unwrap();
    std::fs::write(dir.join("notes.txt"), b"mine").unwrap();
    shim.store.forget_now();

    let written = deletion(
        shim.store.files(),
        &Adopted::default(),
        &["out/member.txt".to_owned()],
        None,
    );
    delete_written(&written);
    let left: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(left, ["notes.txt"]);

    std::fs::remove_file(dir.join("notes.txt")).unwrap();
    delete_written(&written);
    assert!(!dir.exists());
}

// A file that was where the torrent's files go before Weaver opened it is
// never deleted with them, nor is a folder that was there: only what Weaver
// made goes, and each folder it made that this leaves empty.
#[test]
fn deleting_a_torrents_files_leaves_what_was_there_before_it() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("kept")).unwrap();
    std::fs::write(dir.path().join("kept/a.bin"), b"the operator's").unwrap();
    std::fs::create_dir_all(dir.path().join("empty")).unwrap();
    let names = [
        "kept/a.bin".to_owned(),
        "kept/b.bin".to_owned(),
        "made/c.bin".to_owned(),
        "empty/d.bin".to_owned(),
    ];
    let adopted = files::adopted(dir.path(), &names);
    assert_eq!(adopted.files, ["kept/a.bin"]);
    assert!(!adopted.all_files);

    let shim = shim_in(
        dir,
        1024,
        names.iter().map(|name| file(name, 600)).collect(),
    );
    let torrent = noise(2400, 5);
    for piece in 0..3 {
        shim.complete(piece, &torrent);
    }
    let dir = shim.dir.path().to_path_buf();
    shim.store.forget_now();
    delete_written(&deletion(shim.store.files(), &adopted, &[], None));

    let mut left: Vec<_> = walkdir(&dir);
    left.sort();
    assert_eq!(left, ["empty", "kept", "kept/a.bin"]);
    assert!(dir.exists());
}

// libtorrent `storage_utils::delete_one_file`: a torrent's file is removed
// as a file. A folder now where one of its files was is not the torrent's,
// and stays with all it holds; what unpacking placed goes as a file, and
// the folders that leaves empty go after it.
#[test]
fn deleting_a_torrents_files_leaves_a_folder_where_a_file_was() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 500)]);
    let torrent = noise(1500, 6);
    for piece in 0..2 {
        shim.complete(piece, &torrent);
    }
    let dir = shim.dir.path().to_path_buf();
    shim.store.forget_now();
    std::fs::remove_file(dir.join("a.bin")).unwrap();
    std::fs::create_dir_all(dir.join("a.bin/deep")).unwrap();
    std::fs::write(dir.join("a.bin/deep/photo.jpg"), b"the operator's").unwrap();
    std::fs::create_dir_all(dir.join("out/inner")).unwrap();
    std::fs::write(dir.join("out/inner/member.txt"), b"unpacked").unwrap();

    delete_written(&deletion(
        shim.store.files(),
        &Adopted::default(),
        &["out/inner/member.txt".to_owned()],
        None,
    ));
    let mut left = walkdir(&dir);
    left.sort();
    assert_eq!(left, ["a.bin", "a.bin/deep", "a.bin/deep/photo.jpg"]);
    assert_eq!(
        std::fs::read(dir.join("a.bin/deep/photo.jpg")).unwrap(),
        b"the operator's"
    );
}

// What unpacking placed is a file, and goes only as one (`SECURITY.md`,
// "Deletes are narrow"). A folder someone put where the row names an
// unpacked file stays with all it holds, and so does a link there and what
// it points at; the history row lists neither.
#[test]
fn deleting_a_torrents_files_leaves_a_folder_or_link_where_an_output_was() {
    let shim = shim(1024, vec![file("a.bin", 1000)]);
    let torrent = noise(1000, 7);
    shim.complete(0, &torrent);
    let dir = shim.dir.path().to_path_buf();
    shim.store.forget_now();
    std::fs::create_dir_all(dir.join("movie.mkv/deep")).unwrap();
    std::fs::write(dir.join("movie.mkv/deep/photo.jpg"), b"the operator's").unwrap();
    std::fs::write(dir.join("real.txt"), b"unpacked").unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    std::fs::write(elsewhere.path().join("x.bin"), b"x").unwrap();
    let outputs = vec![
        "movie.mkv".to_owned(),
        "real.txt".to_owned(),
        #[cfg(unix)]
        "linked".to_owned(),
    ];
    #[cfg(unix)]
    std::os::unix::fs::symlink(elsewhere.path(), dir.join("linked")).unwrap();

    let written = deletion(shim.store.files(), &Adopted::default(), &outputs, None);
    let listed: Vec<String> = crate::torrents::service::listed_files(&written)
        .into_iter()
        .map(|file| file.path)
        .collect();
    assert_eq!(
        listed,
        [
            dir.join("a.bin").to_string_lossy().into_owned(),
            dir.join("real.txt").to_string_lossy().into_owned()
        ],
        "only the files are listed, each to go as a file"
    );
    delete_written(&written);
    assert!(!dir.join("a.bin").exists());
    assert!(!dir.join("real.txt").exists());
    assert_eq!(
        std::fs::read(dir.join("movie.mkv/deep/photo.jpg")).unwrap(),
        b"the operator's"
    );
    #[cfg(unix)]
    {
        assert!(dir.join("linked").exists(), "the link stays");
        assert!(elsewhere.path().join("x.bin").exists());
    }
}

// A live removal with files never leaves the torrent's folder through a
// link inside it: a listed file and an unpacked one under a folder that has
// become a link stay, with what the link points at, as they do when a start
// finishes the removal (`spare_held`).
#[cfg(unix)]
#[test]
fn deleting_a_torrents_files_follows_no_link_inside_its_folder() {
    let shim = shim(1024, vec![file("sub/a.bin", 1000), file("b.bin", 500)]);
    let torrent = noise(1500, 8);
    for piece in 0..2 {
        shim.complete(piece, &torrent);
    }
    let dir = shim.dir.path().to_path_buf();
    shim.store.forget_now();
    let elsewhere = tempfile::tempdir().unwrap();
    std::fs::write(elsewhere.path().join("a.bin"), b"someone's").unwrap();
    std::fs::write(elsewhere.path().join("member.txt"), b"someone's").unwrap();
    std::fs::remove_dir_all(dir.join("sub")).unwrap();
    std::os::unix::fs::symlink(elsewhere.path(), dir.join("sub")).unwrap();

    delete_written(&deletion(
        shim.store.files(),
        &Adopted::default(),
        &["sub/member.txt".to_owned()],
        None,
    ));
    assert!(!dir.join("b.bin").exists(), "its own file goes");
    assert_eq!(
        std::fs::read(elsewhere.path().join("a.bin")).unwrap(),
        b"someone's"
    );
    assert_eq!(
        std::fs::read(elsewhere.path().join("member.txt")).unwrap(),
        b"someone's"
    );
    assert!(dir.join("sub").exists(), "the link stays");
}

// Deleting a torrent removes only its listed files and never reaches out
// through a link: a listed file that is a link to one outside goes as the
// link, what it pointed at stays, and a file under a link in the middle of
// its name stays with what that link points at.
#[cfg(unix)]
#[test]
fn deleting_a_torrents_files_never_follows_a_link_outside() {
    let shim = shim(
        1024,
        vec![
            file("a.bin", 500),
            file("mid/deep/b.bin", 500),
            file("c.bin", 500),
        ],
    );
    let torrent = noise(1500, 9);
    for piece in 0..2 {
        shim.complete(piece, &torrent);
    }
    let dir = shim.dir.path().to_path_buf();
    shim.store.forget_now();
    let elsewhere = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(elsewhere.path().join("deep")).unwrap();
    std::fs::write(elsewhere.path().join("target.bin"), b"someone's").unwrap();
    std::fs::write(elsewhere.path().join("deep/b.bin"), b"someone's").unwrap();
    std::fs::remove_file(dir.join("a.bin")).unwrap();
    std::os::unix::fs::symlink(elsewhere.path().join("target.bin"), dir.join("a.bin")).unwrap();
    std::fs::remove_dir_all(dir.join("mid")).unwrap();
    std::fs::create_dir_all(dir.join("mid")).unwrap();
    std::os::unix::fs::symlink(elsewhere.path().join("deep"), dir.join("mid/deep")).unwrap();

    delete_written(&deletion(shim.store.files(), &Adopted::default(), &[], None));
    assert!(
        std::fs::symlink_metadata(dir.join("a.bin")).is_err(),
        "the link at a listed name goes, as a link"
    );
    assert!(!dir.join("c.bin").exists(), "its own file goes");
    assert_eq!(
        std::fs::read(elsewhere.path().join("target.bin")).unwrap(),
        b"someone's"
    );
    assert_eq!(
        std::fs::read(elsewhere.path().join("deep/b.bin")).unwrap(),
        b"someone's"
    );
    assert!(dir.join("mid/deep").exists(), "the link in the middle stays");
}

// Every path under the directory, relative to it.
fn walkdir(dir: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(at) = stack.pop() {
        for entry in std::fs::read_dir(&at).unwrap() {
            let path = entry.unwrap().path();
            found.push(
                path.strip_prefix(dir)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            );
            if path.is_dir() {
                stack.push(path);
            }
        }
    }
    found
}

// libtorrent `torrent::on_piece_hashed`: a piece the check found on disk is
// had (`we_have`) and nothing is written. Each piece adopted from where the
// files were placed is had, and the file keeps the bytes it had; one its
// hold refuses, seed mode having ended, is not.
#[test]
fn an_adopted_piece_is_had_and_not_written_back() {
    let shim = shim(1024, vec![file("a.bin", 3000)]);
    let on_disk = noise(3000, 5);
    std::fs::write(shim.dir.path().join("a.bin"), &on_disk).unwrap();
    assert!(shim.store.adopt(0, |_| None).is_none());
    assert!(!shim.store.has(0));
    for piece in 0..shim.layout.piece_count() {
        shim.store
            .adopt(piece, |take| Some(take()))
            .unwrap()
            .unwrap();
        assert!(shim.store.has(piece), "piece {piece}");
    }
    assert_eq!(shim.store.count(), 3);
    assert_eq!(
        std::fs::read(shim.dir.path().join("a.bin")).unwrap(),
        on_disk
    );
}

// libtorrent `torrent::move_storage`: a torrent's files move from where they
// sit to the new save path, and the torrent goes on from there. Files move
// part-written, a file not yet begun has nothing to move, a piece had is
// served from the new place, and the pieces still to come are written
// there.
#[test]
fn files_move_part_written_and_the_torrent_goes_on_from_there() {
    let shim = shim(
        1024,
        vec![
            file("a.bin", 1000),
            file("sub/b.bin", 2500),
            file("c.bin", 700),
        ],
    );
    let torrent = noise(4200, 7);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let placed = shim.dir.path().to_path_buf();
    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().join("moved");
    assert!(placed.join("sub/b.bin").exists());
    assert!(!placed.join("c.bin").exists());
    std::fs::create_dir_all(&moved).unwrap();

    let went = move_plain(&shim.store, moved.clone());
    assert_eq!((went.to, went.kept.len()), (moved.clone(), 0));
    assert!(!placed.join("a.bin").exists());
    assert!(!placed.join("sub").exists());
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), torrent[..1000]);
    assert_eq!(shim.read(0), torrent[..1024]);
    assert_eq!(shim.store.files().path(2), Some(moved.join("c.bin")));

    for piece in 2..5 {
        shim.complete(piece, &torrent);
    }
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), torrent[..1000]);
    assert_eq!(
        std::fs::read(moved.join("sub/b.bin")).unwrap(),
        torrent[1000..3500]
    );
    assert_eq!(std::fs::read(moved.join("c.bin")).unwrap(), torrent[3500..]);
    assert_eq!(std::fs::read_dir(&placed).unwrap().count(), 0);
}

// A move of the torrent's placed files that keeps a file already there, for
// a torrent with nothing unpacked beside them, its row written by no one.
fn move_plain(store: &TorrentStore, to: PathBuf) -> crate::torrents::service::Moved {
    let moving = Listed {
        job_id: JobId(1),
        storage: None,
        outputs: &[],
        existing: Existing::Keep,
        to_made: false,
        forgot: false,
    };
    move_listed(store, moving, to, |_| Ok(())).unwrap()
}

// libtorrent `move_flags_t::dont_replace`: a file already at the new place
// stays, the torrent's own copy of it stays where it was, the other files
// move, and the store reads every file from the new place; the move tells
// that it kept one.
#[test]
fn a_file_already_where_the_files_go_is_kept() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1000)]);
    let torrent = noise(2000, 8);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let placed = shim.dir.path().to_path_buf();
    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().to_path_buf();
    std::fs::write(moved.join("b.bin"), b"theirs").unwrap();

    let went = move_plain(&shim.store, moved.clone());
    assert_eq!(
        (went.to, went.kept),
        (moved.clone(), vec!["b.bin".to_owned()])
    );
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), torrent[..1000]);
    assert!(!placed.join("a.bin").exists());
    assert_eq!(std::fs::read(moved.join("b.bin")).unwrap(), b"theirs");
    assert_eq!(
        std::fs::read(placed.join("b.bin")).unwrap(),
        torrent[1000..]
    );
    assert_eq!(
        [0, 1].map(|file| shim.store.files().path(file)),
        [Some(moved.join("a.bin")), Some(moved.join("b.bin"))]
    );
}

// The move of `store`'s placed files to `to` with `outputs` beside them,
// its row written by `record`.
fn move_with(
    store: &TorrentStore,
    to: &Path,
    outputs: &[&str],
    existing: Existing,
    record: impl FnOnce(&crate::torrents::service::Moved) -> Result<(), crate::StateError>,
) -> Result<crate::torrents::service::Moved, crate::torrents::service::NotMoved> {
    let outputs: Vec<String> = outputs.iter().map(|name| (*name).to_owned()).collect();
    let moving = Listed {
        job_id: JobId(1),
        storage: None,
        outputs: &outputs,
        existing,
        to_made: std::fs::symlink_metadata(to).is_err(),
        forgot: false,
    };
    std::fs::create_dir_all(to).unwrap();
    move_listed(store, moving, to.to_path_buf(), record)
}

// A move into a folder of the torrent's own where an unpacked file sits at
// the very name one of the torrent's files moves to: the unpacked file
// moves out of the way first, to its own name under the new place, and the
// torrent's file then takes the name. Neither replaces the other, in either
// mode, and none is taken for a file kept there. `output` names the
// unpacked file; on a volume that folds names `b/X.BIN` is the name
// `b/x.bin`, and is moved out of the way as that one is.
#[test]
fn an_unpacked_file_where_a_listed_file_goes_moves_out_of_its_way() {
    for existing in [Existing::Replace, Existing::Keep] {
        for output in ["b/x.bin", "b/X.BIN"] {
            let shim = shim(1024, vec![file("x.bin", 1000)]);
            let torrent = noise(1000, 21);
            shim.complete(0, &torrent);
            let placed = shim.dir.path().to_path_buf();
            let to = placed.join("b");
            std::fs::create_dir_all(&to).unwrap();
            std::fs::write(placed.join(output), b"unpacked").unwrap();

            let went = move_with(&shim.store, &to, &[output], existing, |_| Ok(())).unwrap();
            assert!(went.kept.is_empty(), "{existing:?} {output}");
            assert_eq!(went.outputs, [output], "{existing:?} {output}");
            assert_eq!(std::fs::read(to.join("x.bin")).unwrap(), torrent);
            assert_eq!(std::fs::read(to.join(output)).unwrap(), b"unpacked");
            assert!(!placed.join("x.bin").exists());
            assert_eq!(shim.read(0), torrent[..1000]);
        }
    }
}

// A move that ends unrecorded puts back what it moved by moves that never
// replace. A file of the torrent's and an unpacked file whose old names
// someone took meanwhile stay at the new place; the files now at the old
// names are as they were put there, and the failure names both, so the row
// drops the output and adopts the listed name.
#[test]
fn a_put_back_never_replaces_and_names_what_stayed() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1000)]);
    let torrent = noise(2000, 22);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let placed = shim.dir.path().to_path_buf();
    std::fs::write(placed.join("o.mkv"), b"unpacked").unwrap();
    std::fs::write(placed.join("p.mkv"), b"also unpacked").unwrap();
    let moved_dir = tempfile::tempdir().unwrap();
    let to = moved_dir.path().join("new");

    let taken = placed.clone();
    let not_moved = move_with(
        &shim.store,
        &to,
        &["o.mkv", "p.mkv"],
        Existing::Keep,
        |moved| {
            assert_eq!(moved.outputs, ["o.mkv", "p.mkv"]);
            std::fs::write(taken.join("o.mkv"), b"theirs").unwrap();
            std::fs::write(taken.join("b.bin"), b"theirs too").unwrap();
            Err(crate::StateError::Database("refused".to_owned()))
        },
    )
    .unwrap_err();
    assert_eq!(not_moved.stranded, ["b.bin"]);
    assert_eq!(not_moved.outputs, ["p.mkv"]);
    assert_eq!(std::fs::read(placed.join("o.mkv")).unwrap(), b"theirs");
    assert_eq!(std::fs::read(placed.join("b.bin")).unwrap(), b"theirs too");
    assert_eq!(std::fs::read(to.join("o.mkv")).unwrap(), b"unpacked");
    assert_eq!(std::fs::read(to.join("b.bin")).unwrap(), torrent[1000..]);
    assert_eq!(
        std::fs::read(placed.join("p.mkv")).unwrap(),
        b"also unpacked"
    );
    assert_eq!(
        std::fs::read(placed.join("a.bin")).unwrap(),
        torrent[..1000]
    );
    assert_eq!(shim.store.files().dir(), placed, "read where it was");
    // A file there is not the torrent's now, and its pieces were not
    // forgotten for the move: no read is made of its files, of the upload
    // of a piece to a peer as of any other, until they are.
    assert!(shim.store.files().is_shut());
    let mut buf = [0; 16];
    let refused = shim.storage.pread_exact(1, 0, &mut buf).unwrap_err();
    assert!(
        format!("{refused:#}").contains("not read until"),
        "{refused:#}"
    );
    assert!(shim.storage.pread_exact(0, 0, &mut buf).is_err());
    assert!(
        weaver_torrent::storage::FilesShut::refused_in(&refused),
        "a check is told the files are shut, not that a piece is missing"
    );
    // Whatever has every piece forgotten opens them.
    shim.storage.on_check_everything().unwrap();
    shim.storage.pread_exact(0, 0, &mut buf).unwrap();
    assert_eq!(buf, torrent[..16]);
}

// A move that kept a file it had not seen at the new place before it began,
// so with the torrent's pieces not forgotten for it, ends with the store's
// files shut to every read: what is at that name is someone else's, and
// the store would read it as the torrent's. They open once the pieces are
// forgotten. A move whose pieces were forgotten before it leaves them open,
// with nothing had to read.
#[test]
fn a_move_that_kept_a_file_unseen_shuts_the_files_until_the_pieces_are_forgotten() {
    for forgot in [false, true] {
        let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1000)]);
        let torrent = noise(2000, 8);
        shim.complete(0, &torrent);
        shim.complete(1, &torrent);
        let moved_dir = tempfile::tempdir().unwrap();
        let moved = moved_dir.path().to_path_buf();
        // As long as the torrent's own, so only the fence stands between
        // it and a read.
        let theirs = noise(1000, 99);
        std::fs::write(moved.join("b.bin"), &theirs).unwrap();
        let moving = Listed {
            job_id: JobId(1),
            storage: None,
            outputs: &[],
            existing: Existing::Keep,
            to_made: false,
            forgot,
        };
        let went = move_listed(&shim.store, moving, moved.clone(), |_| Ok(())).unwrap();
        assert_eq!(went.kept, ["b.bin"]);
        assert_eq!(shim.store.files().dir(), moved);
        assert_eq!(shim.store.files().is_shut(), !forgot);
        let mut buf = [0; 16];
        for file in [0, 1] {
            let read = shim.storage.pread_exact(file, 0, &mut buf);
            assert_eq!(read.is_err(), !forgot, "file {file}: {read:?}");
        }
        // Shut files are vouched for by nothing kept for the next run.
        assert_eq!(shim.store.kept().checking, !forgot);
        if forgot {
            continue;
        }
        // The one place every piece is forgotten opens them: a recheck,
        // the start after an error and the move's own check all pass it.
        assert_eq!(shim.store.count(), 2);
        shim.storage.on_check_everything().unwrap();
        assert_eq!(shim.store.count(), 0);
        assert!(!shim.store.files().is_shut());
        shim.storage.pread_exact(1, 24, &mut buf).unwrap();
        assert_eq!(buf, theirs[24..40], "what is there, read only now");
    }
}

// A torrent whose files a move left shut joins a session before anything
// forgot its pieces: it hands that session no piece as had, so the session
// checks them all, and its files are read by that check.
#[test]
fn files_left_shut_join_a_session_with_every_piece_to_check() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1000)]);
    let torrent = noise(2000, 8);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().to_path_buf();
    std::fs::write(moved.join("b.bin"), noise(1000, 99)).unwrap();
    let moving = Listed {
        job_id: JobId(1),
        storage: None,
        outputs: &[],
        existing: Existing::Keep,
        to_made: false,
        forgot: false,
    };
    move_listed(&shim.store, moving, moved, |_| Ok(())).unwrap();
    assert!(shim.store.files().is_shut());
    assert_eq!(shim.store.count(), 2);

    assert!(shim.store.initial_have(false).is_none());
    assert_eq!(shim.store.count(), 0, "no piece is had unchecked");
    assert!(!shim.store.files().is_shut());
    let mut buf = [0; 16];
    shim.storage.pread_exact(0, 0, &mut buf).unwrap();
    assert_eq!(buf, torrent[..16]);
}

// What was at the new place before a move is told by the move and outlives
// it: the place itself only when it was there, a folder there that only
// unpacked files go in, and a folder of the torrent's own files that was
// there. A move that ends unrecorded removes the folders it made and none
// that was there.
#[test]
fn folders_at_the_new_place_before_a_move_are_told_and_stay() {
    let files = || vec![file("in/a.bin", 1000), file("made/b.bin", 1000)];
    let unpacked = |placed: &Path| {
        std::fs::create_dir_all(placed.join("subs")).unwrap();
        std::fs::create_dir_all(placed.join("extras")).unwrap();
        std::fs::write(placed.join("subs/a.srt"), b"subs").unwrap();
        std::fs::write(placed.join("extras/e.txt"), b"extra").unwrap();
    };
    let outputs = ["subs/a.srt", "extras/e.txt"];
    let torrent = noise(2000, 23);

    // A place the move makes, with nothing in it before.
    let shim_new = shim(1024, files());
    shim_new.complete(0, &torrent);
    shim_new.complete(1, &torrent);
    unpacked(shim_new.dir.path());
    let root = tempfile::tempdir().unwrap();
    let to = root.path().join("new");
    let went = move_with(&shim_new.store, &to, &outputs, Existing::Keep, |_| Ok(())).unwrap();
    assert!(went.dirs.is_empty(), "{:?}", went.dirs);

    // A place that was there, with a folder of the torrent's files and one
    // only an unpacked file goes in.
    let shim_there = shim(1024, files());
    shim_there.complete(0, &torrent);
    shim_there.complete(1, &torrent);
    unpacked(shim_there.dir.path());
    let to = root.path().join("there");
    std::fs::create_dir_all(to.join("in")).unwrap();
    std::fs::create_dir_all(to.join("subs")).unwrap();
    let went = move_with(&shim_there.store, &to, &outputs, Existing::Keep, |_| Ok(())).unwrap();
    assert_eq!(went.dirs, ["", "in", "subs"]);

    // The same place, the move unrecorded: what it made there goes, and
    // the folders that were there stay, empty as they were.
    let shim_back = shim(1024, files());
    shim_back.complete(0, &torrent);
    shim_back.complete(1, &torrent);
    unpacked(shim_back.dir.path());
    let to = root.path().join("back");
    std::fs::create_dir_all(to.join("in")).unwrap();
    std::fs::create_dir_all(to.join("subs")).unwrap();
    let not_moved = move_with(&shim_back.store, &to, &outputs, Existing::Keep, |_| {
        Err(crate::StateError::Database("refused".to_owned()))
    })
    .unwrap_err();
    assert!(not_moved.stranded.is_empty());
    assert_eq!(not_moved.outputs, outputs);
    assert!(to.join("in").is_dir() && to.join("subs").is_dir());
    assert!(!to.join("made").exists() && !to.join("extras").exists());
    assert_eq!(std::fs::read_dir(to.join("in")).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(to.join("subs")).unwrap().count(), 0);
    let placed = shim_back.dir.path();
    assert_eq!(std::fs::read(placed.join("subs/a.srt")).unwrap(), b"subs");
    assert_eq!(
        std::fs::read(placed.join("in/a.bin")).unwrap(),
        torrent[..1000]
    );
}

// A file is read through one shared handle, which never outlives a move of
// the files: once they move, a read opens the file where it is now, not
// the one the handle was opened on (libtorrent `file_pool`, which
// `storage::move_storage` clears).
#[test]
fn a_file_is_read_through_one_handle_that_does_not_follow_a_move() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1000)]);
    let torrent = noise(2000, 9);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let mut buf = vec![0; 1000];
    shim.store.files().read(0, 0, &mut buf).unwrap();
    assert_eq!(buf, torrent[..1000]);
    shim.store.files().read(0, 0, &mut buf).unwrap();
    assert_eq!(buf, torrent[..1000]);

    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().to_path_buf();
    move_plain(&shim.store, moved.clone());
    // Another file now stands where a.bin went: a handle kept from before
    // the move would still read the old one.
    std::fs::remove_file(moved.join("a.bin")).unwrap();
    std::fs::write(moved.join("a.bin"), vec![0x5a; 1000]).unwrap();
    shim.store.files().read(0, 0, &mut buf).unwrap();
    assert_eq!(buf, vec![0x5a; 1000]);
}

// The torrent's store, telling the test when every piece is had, how many
// pieces the session handed it, and the thread each was stored on.
struct Watched {
    store: Arc<TorrentStore>,
    whole: watch::Sender<bool>,
    handed: watch::Sender<u32>,
    stored_on: std::sync::Mutex<Vec<(std::thread::ThreadId, Option<String>)>>,
}

impl Watched {
    fn tell(&self) {
        self.whole
            .send_replace(self.store.count() == self.store.layout().piece_count());
    }
}

impl Pieces for Watched {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        self.store.verified(piece, held)?;
        self.tell();
        Ok(())
    }

    fn store(
        self: Arc<Self>,
        piece: u32,
        held: Bytes,
        paid: Option<crate::torrents::writes::Paid>,
        done: crate::torrents::writes::Done,
    ) {
        self.handed.send_modify(|handed| *handed += 1);
        let store = Arc::clone(&self.store);
        store.store(
            piece,
            held,
            paid,
            Box::new(move |written| {
                let thread = std::thread::current();
                self.stored_on
                    .lock()
                    .unwrap()
                    .push((thread.id(), thread.name().map(str::to_owned)));
                self.tell();
                done(written);
            }),
        );
    }

    fn reserve(&self, bytes: u64, room: &Arc<Notify>) -> crate::torrents::writes::VolumeRoom {
        self.store.reserve(bytes, room)
    }

    fn stop_writes(&self) {
        self.store.stop_writes();
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        let held = self.store.failed(piece)?;
        self.tell();
        Ok(held)
    }

    fn checking(&self) -> anyhow::Result<bool> {
        let had = self.store.checking()?;
        self.tell();
        Ok(had)
    }

    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        let found = self.store.found(piece)?;
        self.tell();
        Ok(found)
    }

    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.store.read(file, offset, out)
    }

    // The store's own run read, as the session reads it, so the bench
    // measures one read a run and not one a block.
    fn readv(&self, file: usize, offset: u64, outs: &mut [&mut [u8]]) -> std::io::Result<()> {
        self.store.readv(file, offset, outs)
    }

    fn grow(&self, file: usize, length: u64) -> std::io::Result<()> {
        self.store.grow(file, length)
    }
}

// A session on loopback alone: no DHT, no trackers, no discovery, and
// `blocking` threads for writing and checking pieces.
async fn session(dir: &Path, blocking: Option<usize>) -> Arc<Session> {
    Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
            runtime_worker_threads: blocking,
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..weaver_torrent::SessionOptions::new(std::sync::Arc::new(
                weaver_torrent::network::KernelSockets::new(None).expect("the kernel's sockets"),
            ))
        },
    )
    .await
    .unwrap()
}

// A torrent of three files, seeded by a session of its own.
struct Seeded {
    originals: [(&'static str, Vec<u8>); 3],
    bytes: bytes::Bytes,
    layout: Arc<PieceLayout>,
    seeder: Arc<Session>,
    peer: SocketAddr,
    _dir: tempfile::TempDir,
}

async fn seeded() -> Seeded {
    seeded_with(0).await
}

// The torrent `seeded` makes, of other bytes for each `seed`.
async fn seeded_with(seed: u8) -> Seeded {
    let dir = tempfile::tempdir().unwrap();
    let set = dir.path().join("set");
    std::fs::create_dir(&set).unwrap();
    let originals = [
        ("a.bin", noise(1000, 1 + 3 * seed)),
        ("b.bin", noise(2500, 2 + 3 * seed)),
        ("c.bin", noise(700, 3 + 3 * seed)),
    ];
    for (name, bytes) in &originals {
        std::fs::write(set.join(name), bytes).unwrap();
    }
    let created = create_torrent(
        &set,
        CreateTorrentOptions {
            name: None,
            trackers: Vec::new(),
            piece_length: Some(1024),
            ssl_cert: None,
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    let bytes = created.as_bytes().unwrap();
    let metainfo = torrent_from_bytes(&bytes).unwrap();
    let info = metainfo.info.data.validate().unwrap();
    let layout = Arc::new(layout_of(&info).unwrap());
    assert!(layout.piece_count() > 3);

    let seeder = session(&dir.path().join("session"), None).await;
    let seeding = seeder
        .add_torrent(
            AddTorrent::from_bytes(bytes.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(set.to_string_lossy().into_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    seeding.wait_until_completed().await.unwrap();
    let peer = seeder.listen_addr().unwrap();
    Seeded {
        originals,
        bytes,
        layout,
        seeder,
        peer,
        _dir: dir,
    }
}

// The torrent's store over the layout under `dir`, with the side the
// storage hands pieces to.
fn watched_store(layout: &Arc<PieceLayout>, dir: &Path) -> (Arc<Watched>, watch::Receiver<bool>) {
    watched(store_in(layout, dir, None))
}

fn watched(store: Arc<TorrentStore>) -> (Arc<Watched>, watch::Receiver<bool>) {
    let (whole, in_hand) = watch::channel(false);
    (
        Arc::new(Watched {
            store,
            whole,
            handed: watch::channel(0).0,
            stored_on: std::sync::Mutex::default(),
        }),
        in_hand,
    )
}

// Writers of their own, made off the async runtime, so what follows a
// piece's writes runs on the writer's thread alone.
fn disk_off_runtime() -> Arc<crate::torrents::writes::TorrentDisk> {
    std::thread::spawn(crate::torrents::writes::TorrentDisk::alone)
        .join()
        .unwrap()
}

// The store of a torrent of plain files under `dir`, written by `disk`
// under `key`.
fn store_on(
    disk: &Arc<crate::torrents::writes::TorrentDisk>,
    key: u64,
    layout: &Arc<PieceLayout>,
    dir: &Path,
) -> Arc<TorrentStore> {
    let files = Files::new(layout, dir.to_path_buf()).unwrap();
    Arc::new(TorrentStore::new(
        Arc::clone(layout),
        files,
        None,
        None,
        disk.writes(key, dir),
    ))
}

// Two torrents fetched by two sessions into files on one volume are written
// by that volume's one writer: every piece of both is stored on one thread,
// the volume's writer, and held there, neither session writes a piece of
// either. Once let go, both have every piece in their files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_torrents_in_two_sessions_on_one_volume_share_its_one_writer() {
    // A seeder each: a session takes one connection from an address.
    let seeders = [seeded().await, seeded().await];
    let seeded = &seeders[0];
    let layout = &seeded.layout;
    let disk = disk_off_runtime();
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let stores = [1, 2].map(|key| {
        store_on(
            &disk,
            key,
            layout,
            &dirs[key as usize - 1].path().join("job"),
        )
    });
    let volume = stores[0].writes().volume();
    assert!(Arc::ptr_eq(&volume, &stores[1].writes().volume()));
    let held = disk.writers().hold(&volume);
    let mut watching = Vec::new();
    let mut leechers = Vec::new();
    for ((store, dir), seeded) in stores.iter().zip(&dirs).zip(&seeders) {
        let (watched, in_hand) = watched(Arc::clone(store));
        let factory = PieceStorageFactory::new(
            Arc::clone(layout),
            Arc::clone(&watched) as Arc<dyn Pieces>,
            unbounded(),
        );
        leechers.push(fetch(seeded, dir.path(), None, factory.boxed_factory()).await);
        watching.push((watched, in_hand));
    }
    // Every piece of both handed over, and none written while the writer
    // is held.
    for (watched, _) in &watching {
        watched
            .handed
            .subscribe()
            .wait_for(|handed| *handed == layout.piece_count())
            .await
            .unwrap();
    }
    for (store, dir) in stores.iter().zip(&dirs) {
        assert_eq!(store.count(), 0);
        assert!(!dir.path().join("job").join("a.bin").exists());
    }
    held.open();
    drop(held);
    for (_, in_hand) in &mut watching {
        in_hand.wait_for(|whole| *whole).await.unwrap();
    }
    let threads: std::collections::HashSet<_> = watching
        .iter()
        .flat_map(|(watched, _)| watched.stored_on.lock().unwrap().clone())
        .collect();
    assert_eq!(threads.len(), 1, "{threads:?}");
    let (_, name) = threads.into_iter().next().unwrap();
    assert!(name.unwrap().starts_with("volume-writer"));
    for dir in &dirs {
        for (name, original) in &seeded.originals {
            assert_eq!(
                &std::fs::read(dir.path().join("job").join(name)).unwrap(),
                original
            );
        }
    }
    let total: u64 = layout.files().map(|file| file.length).sum();
    assert_eq!(
        volume
            .written_bytes
            .load(std::sync::atomic::Ordering::Acquire),
        2 * total
    );
    assert_eq!(volume.unwritten_bytes(), 0);
    for leecher in leechers {
        leecher.stop().await;
    }
    for seeded in &seeders {
        seeded.seeder.stop().await;
    }
}

// With the server's storage, the session writes no piece itself: its
// threads hand each piece over and go on, and the volume's writer is the
// one thread that writes it. Held there, the session has handed every
// piece over and nothing is on disk; every byte on disk was written by
// the writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_piece_is_written_on_a_session_thread() {
    let seeded = seeded().await;
    let layout = &seeded.layout;
    let disk = disk_off_runtime();
    let fetch_dir = tempfile::tempdir().unwrap();
    let store = store_on(&disk, 1, layout, &fetch_dir.path().join("job"));
    let volume = store.writes().volume();
    let held = disk.writers().hold(&volume);
    let (watched, mut in_hand) = watched(Arc::clone(&store));
    let factory = PieceStorageFactory::new(
        Arc::clone(layout),
        Arc::clone(&watched) as Arc<dyn Pieces>,
        unbounded(),
    );
    // One thread for the session's disk work: were a piece written there,
    // the held writer would hold it, and the session would hand over no
    // more.
    let leecher = fetch(&seeded, fetch_dir.path(), Some(1), factory.boxed_factory()).await;
    watched
        .handed
        .subscribe()
        .wait_for(|handed| *handed == layout.piece_count())
        .await
        .unwrap();
    assert_eq!(store.count(), 0);
    assert!(!fetch_dir.path().join("job").join("a.bin").exists());
    assert_eq!(
        volume
            .written_bytes
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    held.open();
    drop(held);
    in_hand.wait_for(|whole| *whole).await.unwrap();
    let stored_on = watched.stored_on.lock().unwrap().clone();
    assert_eq!(stored_on.len(), layout.piece_count() as usize);
    assert!(
        stored_on.iter().all(|(_, name)| name
            .as_deref()
            .is_some_and(|name| name.starts_with("volume-writer"))),
        "{stored_on:?}"
    );
    let total: u64 = layout.files().map(|file| file.length).sum();
    assert_eq!(
        volume
            .written_bytes
            .load(std::sync::atomic::Ordering::Acquire),
        total
    );
    for (name, original) in &seeded.originals {
        assert_eq!(
            &std::fs::read(fetch_dir.path().join("job").join(name)).unwrap(),
            original
        );
    }
    leecher.stop().await;
    seeded.seeder.stop().await;
}

// The seeded torrent fetched from its seeder alone, into `factory`.
async fn fetch(
    seeded: &Seeded,
    dir: &Path,
    blocking: Option<usize>,
    factory: BoxStorageFactory,
) -> Arc<Session> {
    let leecher = session(&dir.join("session"), blocking).await;
    fetch_into(&leecher, seeded, dir, factory, false).await;
    leecher
}

// The seeded torrent fetched from its seeder alone by `leecher`, into
// `factory`; added paused when `paused` says so.
async fn fetch_into(
    leecher: &Arc<Session>,
    seeded: &Seeded,
    dir: &Path,
    factory: BoxStorageFactory,
    paused: bool,
) -> Arc<weaver_torrent::ManagedTorrent> {
    leecher
        .add_torrent(
            AddTorrent::from_bytes(seeded.bytes.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(dir.join("unused").to_string_lossy().into_owned()),
                overwrite: true,
                initial_peers: Some(vec![seeded.peer]),
                storage_factory: Some(factory),
                paused,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap()
}

// One session fetches two torrents while the first one's files are being
// moved: its one hash thread never waits on the move. The second torrent's
// pieces are verified and stored while the move is held; the first's are
// verified and kept in memory, and once the move ends they are written
// where the files went, and nothing is written where they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_moving_holds_up_no_other_torrent_of_its_session() {
    // A seeder each: a session takes one connection from an address.
    let seeders = [seeded_with(0).await, seeded_with(1).await];
    let disk = disk_off_runtime();
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let stores: Vec<_> = seeders
        .iter()
        .zip(&dirs)
        .zip([1, 2])
        .map(|((seeded, dir), key)| store_on(&disk, key, &seeded.layout, &dir.path().join("job")))
        .collect();
    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().join("moved");
    let leecher_dir = tempfile::tempdir().unwrap();
    let leecher = session(&leecher_dir.path().join("session"), None).await;
    let watching: Vec<_> = stores
        .iter()
        .map(|store| watched(Arc::clone(store)))
        .collect();
    let factory = |at: usize| {
        PieceStorageFactory::new(
            Arc::clone(&seeders[at].layout),
            Arc::clone(&watching[at].0) as Arc<dyn Pieces>,
            unbounded(),
        )
        .boxed_factory()
    };

    // The first torrent checked and paused before its move: the check
    // reads its files, and reads are not what this is about.
    let first = fetch_into(&leecher, &seeders[0], dirs[0].path(), factory(0), true).await;
    first.wait_until_initialized().await.unwrap();

    // Its move begins, and is held until let go.
    let (entered, in_move) = std::sync::mpsc::channel();
    let (end, ended) = std::sync::mpsc::channel::<()>();
    let moving = {
        let store = Arc::clone(&stores[0]);
        let moved = moved.clone();
        std::thread::spawn(move || {
            let names: Vec<std::path::PathBuf> = store
                .files()
                .names()
                .flatten()
                .map(Path::to_path_buf)
                .collect();
            store.relocate(
                |dir| {
                    entered.send(()).unwrap();
                    ended.recv().unwrap();
                    let moves: Vec<_> = names
                        .iter()
                        .map(|name| (dir.join(name), moved.join(name)))
                        .collect();
                    crate::torrents::relocate::move_files(dir, &moves, Existing::Keep)?;
                    Ok::<_, anyhow::Error>((moved, ()))
                },
                |_| false,
            )
        })
    };
    in_move.recv().unwrap();

    // Both fetch on the one session while the move is held.
    leecher.unpause(&first).await.unwrap();
    let _second = fetch_into(&leecher, &seeders[1], dirs[1].path(), factory(1), false).await;
    let mut watching: Vec<_> = watching.into_iter().collect();

    // While the move is held: every piece of the first torrent verified
    // and handed over, none of them written; the second torrent whole on
    // disk.
    let pieces = seeders[0].layout.piece_count();
    watching[0]
        .0
        .handed
        .subscribe()
        .wait_for(|handed| *handed == pieces)
        .await
        .unwrap();
    watching[1].1.wait_for(|whole| *whole).await.unwrap();
    assert_eq!(stores[0].count(), 0);
    for (name, original) in &seeders[1].originals {
        assert_eq!(
            &std::fs::read(dirs[1].path().join("job").join(name)).unwrap(),
            original
        );
    }

    end.send(()).unwrap();
    moving.join().unwrap().unwrap();
    watching[0].1.wait_for(|whole| *whole).await.unwrap();
    for (name, original) in &seeders[0].originals {
        assert_eq!(&std::fs::read(moved.join(name)).unwrap(), original);
    }
    let old = dirs[0].path().join("job");
    assert!(
        !old.exists() || std::fs::read_dir(&old).unwrap().count() == 0,
        "written where the files were"
    );
    leecher.stop().await;
    for seeded in &seeders {
        seeded.seeder.stop().await;
    }
}

// One session seeds a torrent from its files; the other fetches it from
// that peer alone into its own files through the piece storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_fetched_from_a_loopback_peer_lands_in_its_files_and_seeds_from_them() {
    let seeded = seeded().await;
    let Seeded {
        originals, layout, ..
    } = &seeded;
    let fetch_dir = tempfile::tempdir().unwrap();
    let (watched, mut in_hand) = watched_store(layout, &fetch_dir.path().join("job"));
    let factory = PieceStorageFactory::new(
        Arc::clone(layout),
        Arc::clone(&watched) as Arc<dyn Pieces>,
        unbounded(),
    );
    let leecher = fetch(&seeded, fetch_dir.path(), None, factory.boxed_factory()).await;
    in_hand.wait_for(|whole| *whole).await.unwrap();

    let dir = fetch_dir.path().join("job");
    let mut torrent = Vec::new();
    for (name, original) in originals {
        assert_eq!(&std::fs::read(dir.join(name)).unwrap(), original, "{name}");
    }
    for index in 0..layout.files().count() {
        let name = &layout.files().nth(index).unwrap().name;
        let (_, original) = originals.iter().find(|(file, _)| file == name).unwrap();
        torrent.extend_from_slice(original);
    }
    let posted = PieceStorage::new(Arc::clone(layout), watched as Arc<dyn Pieces>, unbounded());
    for piece in 0..layout.piece_count() {
        let range = layout.piece_range(piece);
        let mut read = Vec::new();
        for span in layout.spans(piece) {
            let mut buf = vec![0xee; span.range.len() as usize];
            posted
                .pread_exact(span.file, span.range.start, &mut buf)
                .unwrap();
            read.extend(buf);
        }
        assert_eq!(
            read,
            torrent[range.start as usize..range.end as usize],
            "piece {piece}"
        );
    }
    leecher.stop().await;
    seeded.seeder.stop().await;
}

// The piece storage as the session sees it, with the first verified piece
// held back until the test lets it go, and every refusal of room counted.
#[derive(Clone)]
struct Gated {
    storage: PieceStorage,
    gate: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
    refused: Arc<watch::Sender<usize>>,
    // The pieces granted room, in order.
    granted: Arc<std::sync::Mutex<Vec<u32>>>,
}

impl TorrentStorage for Gated {
    fn init(
        &mut self,
        shared: &weaver_torrent::ManagedTorrentShared,
        metadata: &weaver_torrent::TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.storage.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.storage.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.storage.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        self.storage.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        self.storage.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.storage.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(self.clone()))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.recv().unwrap();
        }
        self.storage.on_piece_completed(piece)
    }

    fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.storage.on_piece_hash_failed(piece)
    }

    fn on_check_everything(&self) -> anyhow::Result<()> {
        self.storage.on_check_everything()
    }

    fn on_piece_checked(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.storage.on_piece_checked(piece)
    }

    fn reserve_piece(&self, piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        let granted = self.storage.reserve_piece(piece, room);
        if granted {
            self.granted.lock().unwrap().push(piece.get());
        } else {
            self.refused.send_modify(|refused| *refused += 1);
        }
        granted
    }
}

#[derive(Clone)]
struct GatedFactory {
    factory: PieceStorageFactory,
    gate: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
    refused: Arc<watch::Sender<usize>>,
    // The pieces granted room, in order.
    granted: Arc<std::sync::Mutex<Vec<u32>>>,
}

impl StorageFactory for GatedFactory {
    type Storage = Gated;

    fn create(
        &self,
        shared: &weaver_torrent::ManagedTorrentShared,
        metadata: &weaver_torrent::TorrentMetadata,
    ) -> anyhow::Result<Gated> {
        Ok(Gated {
            storage: self.factory.create(shared, metadata)?,
            gate: Arc::clone(&self.gate),
            refused: Arc::clone(&self.refused),
            granted: Arc::clone(&self.granted),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

// With room for one piece and one thread for writing and checking, the
// session is granted a piece, and while that piece is held at its handover
// with the one thread, the next is refused: the session waits without a
// thread of its own. Once the piece lets its room go the session is rung
// and fetches the rest a piece at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_at_its_budget_waits_for_room_and_resumes_when_a_piece_lets_go() {
    let seeded = seeded().await;
    let layout = &seeded.layout;
    let fetch_dir = tempfile::tempdir().unwrap();
    let (watched, mut in_hand) = watched_store(layout, &fetch_dir.path().join("job"));
    let budget = accountant(layout.piece_length());
    let (go, gate) = std::sync::mpsc::channel();
    let (refused, mut refusals) = watch::channel(0);
    let granted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let factory = GatedFactory {
        factory: PieceStorageFactory::new(
            Arc::clone(layout),
            Arc::clone(&watched) as Arc<dyn Pieces>,
            holds(&budget),
        ),
        gate: Arc::new(std::sync::Mutex::new(Some(gate))),
        refused: Arc::new(refused),
        granted: Arc::clone(&granted),
    };
    let leecher = fetch(&seeded, fetch_dir.path(), Some(1), factory.boxed()).await;

    refusals.wait_for(|refused| *refused > 0).await.unwrap();
    assert_eq!(watched.store.count(), 0);
    // The one piece granted is resident, whichever the picker started from.
    let first = granted.lock().unwrap()[0];
    assert_eq!(
        budget.namespace_usage(NAMESPACE).resident,
        layout.piece_range(first).len()
    );
    go.send(()).unwrap();

    in_hand.wait_for(|whole| *whole).await.unwrap();
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    leecher.stop().await;
    seeded.seeder.stop().await;
}

// A piece that fails its hash is dropped with the room it held, the session
// waiting for that room is rung, the store is told, and the storage going
// gives back the rest.
#[tokio::test]
async fn a_piece_that_fails_its_hash_lets_go_of_its_bytes_and_its_room() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 16)]).unwrap());
    let budget = accountant(8);
    let recorder = Arc::new(Recorder::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&recorder) as Arc<dyn Pieces>,
        holds(&budget),
    );
    let lengths = Lengths::new(16, 8).unwrap();
    let [first, second] = [0, 1].map(|piece| lengths.try_validate_piece_index(piece).unwrap());
    let room = Arc::new(Notify::new());
    assert!(storage.reserve_piece(first, &room));
    storage.pwrite_all(0, 0, b"badbytes").unwrap();
    // The budget holds one piece, and nothing is written without room.
    assert!(!storage.reserve_piece(second, &room));
    assert!(storage.pwrite_all(0, 8, b"x").is_err());

    let rung = room.notified();
    storage.on_piece_hash_failed(first).unwrap();
    rung.await;
    assert_eq!(*recorder.failed.lock().unwrap(), [0]);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(storage.pread_exact(0, 0, &mut [0; 8]).is_err());

    assert!(storage.reserve_piece(second, &room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    drop(storage);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}

// A piece an upload holds whole is held against the resident budget a piece
// in flight is: room for it is refused when the budget is full, it holds
// room a piece in flight then cannot have, and once it is let go the room
// is back, the session waiting for it rung.
#[tokio::test]
async fn a_piece_an_upload_holds_whole_is_held_against_the_resident_budget() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 16)]).unwrap());
    let budget = accountant(8);
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::new(Recorder::default()) as Arc<dyn Pieces>,
        holds(&budget),
    );
    let lengths = Lengths::new(16, 8).unwrap();
    let first = lengths.try_validate_piece_index(0).unwrap();
    let room = Arc::new(Notify::new());

    let held = storage.hold_whole_piece(8).unwrap();
    assert_eq!(held.bytes(), 8);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    // The budget is full: neither another upload's piece nor a piece in
    // flight has room.
    assert!(storage.hold_whole_piece(8).is_none());
    assert!(!storage.reserve_piece(first, &room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);

    let rung = room.notified();
    drop(held);
    rung.await;
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(storage.reserve_piece(first, &room));
    assert!(storage.hold_whole_piece(8).is_none());

    // The storage going gives back the piece in flight; nothing else held
    // any room.
    drop(storage);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}

// A piece its volume has no room for is not started: the session's room
// for it is not taken, and it starts once the volume has written enough.
// A storage taken gives back what its volume counted for pieces not
// written.
#[tokio::test]
async fn a_piece_its_volume_refuses_is_not_started_until_the_volume_has_room() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 32)]).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let store = store_in(&layout, &dir.path().join("job"), None);
    let volume = store.writes().volume();
    // Room for one piece of eight, less the share its writes keep.
    volume.set_buffer_limits(16, 16);
    let budget = accountant(u64::MAX);
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&store) as Arc<dyn Pieces>,
        holds(&budget),
    );
    let lengths = Lengths::new(32, 8).unwrap();
    let [first, second, third] =
        [0, 1, 2].map(|piece| lengths.try_validate_piece_index(piece).unwrap());
    let room = Arc::new(Notify::new());
    assert!(storage.reserve_piece(first, &room));
    assert_eq!(volume.unwritten_bytes(), 8);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    assert!(!storage.reserve_piece(second, &room));
    // Not started: nothing more held, and no block of it is taken.
    assert_eq!(volume.unwritten_bytes(), 8);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    assert!(storage.pwrite_all(0, 8, b"x").is_err());

    let rung = room.notified();
    storage.pwrite_all(0, 0, b"abcdefgh").unwrap();
    storage.on_piece_completed(first).unwrap();
    rung.await;
    assert_eq!(
        std::fs::read(dir.path().join("job").join("a")).unwrap()[..8],
        *b"abcdefgh"
    );
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(storage.reserve_piece(second, &room));
    assert_eq!(volume.unwritten_bytes(), 8);
    storage.pwrite_all(0, 8, b"ijk").unwrap();

    // Taken at a pause, the storage gives back what the volume counted
    // for the piece it held in memory, which starts again on its next
    // reserve.
    let taken = storage.take().unwrap();
    assert_eq!(volume.unwritten_bytes(), 0);
    drop(taken);
    assert!(storage.reserve_piece(third, &room));
    assert_eq!(volume.unwritten_bytes(), 8);
    drop(storage);
    assert_eq!(volume.unwritten_bytes(), 0);
}

// The store's side that keeps what it is handed, reads the piece back
// through the storage while it is being handed over, and the pieces it was
// told failed. It has nothing on disk.
#[derive(Default)]
struct Recorder {
    storage: std::sync::OnceLock<PieceStorage>,
    handed: std::sync::Mutex<Vec<(u32, Bytes)>>,
    read_while_handed: std::sync::Mutex<Vec<u8>>,
    failed: std::sync::Mutex<Vec<u32>>,
}

impl Pieces for Recorder {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        let storage = self.storage.get().unwrap();
        let mut read = self.read_while_handed.lock().unwrap();
        for (file, length) in [(0, 5), (1, 3)] {
            let mut buf = vec![0; length];
            storage.pread_exact(file, 0, &mut buf)?;
            read.extend(buf);
        }
        self.handed.lock().unwrap().push((piece, held.clone()));
        Ok(())
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.failed.lock().unwrap().push(piece);
        Ok(false)
    }

    fn checking(&self) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn found(&self, _: u32) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn read(&self, _: usize, _: u64, _: &mut [u8]) -> std::io::Result<()> {
        Err(std::io::ErrorKind::NotFound.into())
    }

    fn grow(&self, _: usize, _: u64) -> std::io::Result<()> {
        Ok(())
    }
}

// A verified piece goes to the store as the one buffer its blocks landed
// in and its hash was taken over: the bytes hashed and the bytes handed are
// the same memory, so nothing is copied between the check and the disk,
// and the piece reads from memory until the store has it.
#[test]
fn a_verified_piece_goes_to_the_store_as_the_buffer_it_was_hashed_in() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 5), file("b", 3)]).unwrap());
    let recorder = Arc::new(Recorder::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&recorder) as Arc<dyn Pieces>,
        unbounded(),
    );
    recorder.storage.set(storage.clone()).ok().unwrap();
    let index = Lengths::new(8, 8)
        .unwrap()
        .try_validate_piece_index(0)
        .unwrap();
    assert!(storage.reserve_piece(index, &Arc::default()));
    storage.pwrite_all(0, 0, b"aaaaa").unwrap();
    storage.pwrite_all(1, 0, b"bbb").unwrap();
    let mut hashed = Vec::new();
    assert!(storage.hash_held_piece(index, &mut |bytes| hashed.push(bytes.as_ptr() as usize)));
    storage.on_piece_completed(index).unwrap();

    assert_eq!(*recorder.read_while_handed.lock().unwrap(), b"aaaaabbb");
    let handed = recorder.handed.lock().unwrap();
    assert_eq!(handed.len(), 1);
    let (piece, held) = &handed[0];
    assert_eq!((*piece, &held[..]), (0, &b"aaaaabbb"[..]));
    assert_eq!(hashed, [held.as_ptr() as usize]);
    // Once handed over, the piece is no longer in memory.
    assert!(storage.pread_exact(0, 0, &mut [0; 5]).is_err());
}

// A piece handed on to another peer is written into the room it already
// holds: a steal is never asked for room, and a piece its peer gave back is
// granted again without more. So no write the session makes after a grant
// finds the piece without room.
#[test]
fn a_piece_handed_on_is_written_into_the_room_it_holds() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 5), file("b", 3)]).unwrap());
    let budget = accountant(8);
    let recorder = Arc::new(Recorder::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&recorder) as Arc<dyn Pieces>,
        holds(&budget),
    );
    recorder.storage.set(storage.clone()).ok().unwrap();
    let piece = Lengths::new(8, 8)
        .unwrap()
        .try_validate_piece_index(0)
        .unwrap();
    let room = Arc::new(Notify::new());
    assert!(storage.reserve_piece(piece, &room));
    // The first peer writes half, then the piece is stolen: the thief
    // writes the rest with nothing asked.
    storage.pwrite_all(0, 0, b"abcde").unwrap();
    storage.pwrite_all(1, 0, b"fgh").unwrap();
    // The thief leaves and the piece is queued again: the next peer is
    // granted it with the budget already full, and rewrites it.
    assert!(storage.reserve_piece(piece, &room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    storage.pwrite_all(0, 0, b"ab").unwrap();
    storage.on_piece_completed(piece).unwrap();
    let handed = recorder.handed.lock().unwrap();
    assert_eq!(&handed[0].1[..], b"abcdefgh");
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}

// A whole piece is hashed where its blocks landed: the bytes handed are the
// piece as a read back would find it, across the files it spans, and a
// piece not in memory hands nothing, so the session reads it instead.
#[test]
fn a_piece_in_memory_is_handed_whole_for_hashing_and_one_not_is_not() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1500)]);
    let torrent = noise(2500, 10);
    assert!(
        !shim
            .storage
            .hash_held_piece(shim.index(0), &mut |_| panic!("nothing held"))
    );
    shim.write(0, &torrent);
    let mut handed = Vec::new();
    assert!(
        shim.storage
            .hash_held_piece(shim.index(0), &mut |bytes| handed.extend_from_slice(bytes))
    );
    assert_eq!(handed, torrent[..1024]);
    assert_eq!(handed, shim.read(0));
    shim.storage.on_piece_completed(shim.index(0)).unwrap();
    assert!(
        !shim
            .storage
            .hash_held_piece(shim.index(0), &mut |_| panic!("piece left memory"))
    );
    assert!(shim.storage.pwrite_all(0, 0, &torrent[..16]).is_err());
}

// A torrent of plain files has no tee: nothing of it reaches the graph. Each
// piece is hashed once, from the buffer its blocks landed in, and written
// once, to its files; the store reads nothing of the files to have it.
#[test]
fn a_plain_torrent_is_hashed_once_and_written_once_and_never_reaches_the_graph() {
    let (shim, torrent) = three_files();
    assert!(crate::torrents::tee::Plan::of(&shim.layout).is_none());
    assert!(shim.store.tee().is_none());
    let counted = Arc::new(Counted {
        store: Arc::clone(&shim.store),
        verified: Default::default(),
        reads: Default::default(),
    });
    let storage = PieceStorage::new(
        Arc::clone(&shim.layout),
        Arc::clone(&counted) as Arc<dyn Pieces>,
        unbounded(),
    );
    let mut hashed = 0;
    for piece in 0..5 {
        let index = shim.index(piece);
        assert!(storage.reserve_piece(index, &shim.room));
        let range = shim.layout.piece_range(piece);
        for span in shim.layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &torrent[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
        assert!(storage.hash_held_piece(index, &mut |bytes| hashed += bytes.len()));
        storage.on_piece_completed(index).unwrap();
    }
    assert_eq!(hashed, torrent.len());
    assert_eq!(*counted.verified.lock().unwrap(), torrent.len());
    assert_eq!(*counted.reads.lock().unwrap(), 0);
    let on_disk: u64 = ["a.bin", "b.bin", "c.bin"]
        .iter()
        .map(|name| std::fs::metadata(shim.dir.path().join(name)).unwrap().len())
        .sum();
    assert_eq!(on_disk, torrent.len() as u64);
}

// The store, with the bytes handed to it and the reads of its files
// counted.
struct Counted {
    store: Arc<TorrentStore>,
    verified: std::sync::Mutex<usize>,
    reads: std::sync::Mutex<usize>,
}

impl Pieces for Counted {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        *self.verified.lock().unwrap() += held.len();
        self.store.verified(piece, held)
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.store.failed(piece)
    }

    fn checking(&self) -> anyhow::Result<bool> {
        self.store.checking()
    }

    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        self.store.found(piece)
    }

    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        *self.reads.lock().unwrap() += 1;
        self.store.read(file, offset, out)
    }

    fn grow(&self, file: usize, length: u64) -> std::io::Result<()> {
        self.store.grow(file, length)
    }
}

// A piece shared by a wanted file and one not downloaded writes only the
// wanted one: the other's share goes to the side store, from where the piece
// reads back whole, and its file and folder are never made. Downloading the
// file again copies its share out into it, and the side store keeps nothing
// then (libtorrent `mmap_storage::writev` and `set_file_priority`).
#[test]
fn an_ignored_files_share_of_a_piece_goes_to_the_side_store_not_the_file() {
    let work = tempfile::tempdir().unwrap();
    let shim = wanted_then_ignored(work.path(), tempfile::tempdir().unwrap(), None);
    let torrent = noise(3584, 11);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    assert_eq!(
        std::fs::read(shim.dir.path().join("a.bin")).unwrap(),
        torrent[..1536]
    );
    assert!(!shim.dir.path().join("sub").exists());
    assert!(shim.store.has(1));
    assert_eq!(shim.read(1), torrent[1024..2048]);
    assert_eq!(shim.store.side_pieces(), [1]);
    // A file set aside is never made, even at its length.
    shim.storage.ensure_file_length(1, 2048).unwrap();
    assert!(!shim.dir.path().join("sub").exists());
    assert!(!shim.store.whole_names().contains(&"sub/b.bin".to_owned()));

    assert!(shim.store.needs_copy(&[false, false]));
    shim.store.relevel(&[false, false]).unwrap();
    assert!(shim.store.side_pieces().is_empty());
    assert_eq!(
        std::fs::read(shim.dir.path().join("sub/b.bin")).unwrap(),
        torrent[1536..2048]
    );
    assert_eq!(shim.read(1), torrent[1024..2048]);
    for piece in 2..4 {
        shim.complete(piece, &torrent);
    }
    assert_eq!(
        std::fs::read(shim.dir.path().join("sub/b.bin")).unwrap(),
        torrent[1536..]
    );
}

// A file already at the path of one not downloaded, which the torrent did
// not write, is left as it is: the torrent's share of it goes to the side
// store, and the torrent reads its own bytes back from there.
#[test]
fn a_file_at_an_ignored_path_is_never_written() {
    let work = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("sub")).unwrap();
    std::fs::write(dir.path().join("sub/b.bin"), b"the operator's own").unwrap();
    let shim = wanted_then_ignored(work.path(), dir, None);
    let torrent = noise(3584, 12);
    shim.complete(1, &torrent);
    assert_eq!(
        std::fs::read(shim.dir.path().join("sub/b.bin")).unwrap(),
        b"the operator's own"
    );
    assert_eq!(shim.read(1), torrent[1024..2048]);
}

// The side store outlives the run: a store made again from what it kept
// reads the piece back from it, loses no piece to the file not being
// there, and a check of every piece finds the piece from it. A piece the
// check does not find leaves the side store with it.
#[test]
fn a_side_store_is_found_again_and_checked_from() {
    let work = tempfile::tempdir().unwrap();
    let shim = wanted_then_ignored(work.path(), tempfile::tempdir().unwrap(), None);
    let torrent = noise(3584, 13);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let kept = shim.store.kept();
    let Shim { store, dir, .. } = shim;
    drop(store);

    let again = wanted_then_ignored(work.path(), dir, Some(&kept));
    assert_eq!(again.store.drop_unbacked(), Some(0));
    assert_eq!(again.store.count(), 2);
    assert_eq!(again.read(1), torrent[1024..2048]);

    assert!(Pieces::checking(&*again.store).unwrap());
    assert_eq!(again.read(1), torrent[1024..2048]);
    assert!(Pieces::found(&*again.store, 1).unwrap());
    assert!(again.store.has(1));
    assert_eq!(again.store.side_pieces(), [1]);
    assert!(Pieces::failed(&*again.store, 1).unwrap());
    assert!(again.store.side_pieces().is_empty());
    let mut out = vec![0; 512];
    assert!(again.storage.pread_exact(1, 0, &mut out).is_err());
}

// A side store left short, as by a crash while it is first written, is no
// side store: the torrent's store opens with it empty, and the pieces kept
// are checked again, since what it held is gone. Piece 1, kept with its
// share of b there alone, is had no longer and does not read; piece 0 is
// found again from a.bin. Piece 1 fetched again goes to a fresh side store
// and reads back whole, there and once the store is made again.
#[test]
fn a_short_side_store_opens_empty_and_its_pieces_are_checked_again() {
    let work = tempfile::tempdir().unwrap();
    let shim = wanted_then_ignored(work.path(), tempfile::tempdir().unwrap(), None);
    let torrent = noise(3584, 16);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let kept = shim.store.kept();
    let side = shim.store.side_path().unwrap();
    let Shim { store, dir, .. } = shim;
    drop(store);
    std::fs::write(&side, [0x48, 0x53, 0x44]).unwrap();

    let again = wanted_then_ignored(work.path(), dir, Some(&kept));
    assert_eq!(again.store.side_reset(), Some("its head is short"));
    assert!(again.store.side_pieces().is_empty());
    assert!(!side.exists());
    assert!(!again.store.has(0) && !again.store.has(1));
    assert!(again.store.initial_have(false).is_none());
    assert!(again.store.kept().checking);
    assert_eq!(again.store.drop_unbacked(), Some(0));
    let mut out = vec![0; 512];
    assert!(again.storage.pread_exact(1, 0, &mut out).is_err());
    assert_eq!(again.read(0), torrent[..1024]);
    assert!(Pieces::found(&*again.store, 0).unwrap());

    again.complete(1, &torrent);
    assert!(again.store.has(1));
    assert_eq!(again.store.side_pieces(), [1]);
    assert_eq!(again.read(1), torrent[1024..2048]);
    assert!(!again.dir.path().join("sub").exists());
    let kept = again.store.kept();
    let Shim { store, dir, .. } = again;
    drop(store);
    let third = wanted_then_ignored(work.path(), dir, Some(&kept));
    assert!(third.store.side_reset().is_none());
    assert_eq!(third.store.side_pieces(), [1]);
    assert_eq!(third.read(1), torrent[1024..2048]);
}

// A side store whose piece the pieces kept do not have holds nothing the
// torrent reads: it is dropped as the store opens.
#[test]
fn a_side_store_piece_not_kept_is_dropped() {
    let work = tempfile::tempdir().unwrap();
    let shim = wanted_then_ignored(work.path(), tempfile::tempdir().unwrap(), None);
    let torrent = noise(3584, 14);
    shim.complete(1, &torrent);
    let Shim { store, dir, .. } = shim;
    drop(store);
    let again = wanted_then_ignored(work.path(), dir, None);
    assert!(again.store.side_pieces().is_empty());
}

// The side store is not among the torrent's files: a move of them leaves
// it where it is, and the piece still reads whole from the new place.
#[test]
fn a_move_keeps_the_side_store() {
    let work = tempfile::tempdir().unwrap();
    let shim = wanted_then_ignored(work.path(), tempfile::tempdir().unwrap(), None);
    let torrent = noise(3584, 15);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().join("moved");
    std::fs::create_dir_all(&moved).unwrap();
    move_plain(&shim.store, moved.clone());
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), torrent[..1536]);
    assert!(!moved.join("sub").exists());
    assert_eq!(shim.read(1), torrent[1024..2048]);
    let side = shim.store.side_path().unwrap();
    assert!(side.starts_with(work.path()));
    assert!(side.exists());
}
