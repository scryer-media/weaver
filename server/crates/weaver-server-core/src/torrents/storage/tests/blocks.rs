// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Pieces larger than the resident budget: written block by block as they
// arrive, hashed from disk, had once the hash passed.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use weaver_torrent::storage::TorrentStorage;

use super::*;

const PIECE: u64 = 64 << 10;
const BLOCK: u64 = 16 << 10;
// Room for two blocks: no piece fits whole.
const BUDGET: u64 = 32 << 10;

// a.bin is torrent bytes 0..100000 and b.bin 100000..160000: piece 1 is the
// end of a.bin and the start of b.bin.
fn two_files() -> Arc<PieceLayout> {
    Arc::new(
        PieceLayout::new(PIECE, vec![file("a.bin", 100_000), file("b.bin", 60_000)]).unwrap(),
    )
}

fn shim_over(dir: tempfile::TempDir, holds: Holds, kept: Option<&Kept>) -> Shim {
    let layout = two_files();
    let store = store_in(&layout, dir.path(), kept);
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&store) as Arc<dyn Pieces>,
        holds,
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
    // The piece's first `upto` blocks written as the session writes them:
    // room for the piece, then a block at a time, each cut at a file's end.
    // Block `bad` is written with its first byte wrong.
    fn write_blocks(&self, piece: u32, torrent: &[u8], upto: u64, bad: Option<u64>) {
        assert!(self.storage.reserve_piece(self.index(piece), &self.room));
        let start = self.layout.piece_range(piece).start;
        for span in self.layout.spans(piece) {
            let end = span.at + span.range.len();
            let mut at = span.at;
            while at < end {
                let block = at / BLOCK;
                if block >= upto {
                    return;
                }
                let stop = end.min((block + 1) * BLOCK);
                let mut bytes = torrent[(start + at) as usize..(start + stop) as usize].to_vec();
                if bad == Some(block) {
                    bytes[0] ^= 0xff;
                }
                self.storage
                    .pwrite_all(span.file, span.range.start + (at - span.at), &bytes)
                    .unwrap();
                at = stop;
            }
        }
    }

    fn files(&self) -> [Vec<u8>; 2] {
        ["a.bin", "b.bin"].map(|name| std::fs::read(self.dir.path().join(name)).unwrap())
    }
}

// A piece larger than the budget holds one block's room while it arrives,
// and each block is on disk as it arrives, before the piece verifies: the
// piece is read from there, as its hash is. Not had until it verified;
// once it did, it is had and its room is back.
#[test]
fn an_oversized_piece_is_written_block_by_block_and_had_once_it_verified() {
    let budget = accountant(BUDGET);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    let torrent = noise(160_000, 9);
    assert!(!shim.storage.writes_to_memory());

    shim.write_blocks(1, &torrent, u64::MAX, None);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, BLOCK);
    assert_eq!(shim.store.count(), 0);
    assert!(!shim.store.has(1));
    let [a, b] = shim.files();
    assert_eq!(&a[65_536..100_000], &torrent[65_536..100_000]);
    assert_eq!(&b[..31_072], &torrent[100_000..131_072]);
    assert_eq!(shim.read(1), torrent[65_536..131_072]);

    shim.storage.on_piece_completed(shim.index(1)).unwrap();
    assert!(shim.store.has(1));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert_eq!(shim.storage.resident_peak(), BLOCK);
    assert!(shim.storage.resident_peak() <= BUDGET);
}

// The same pieces, the same bytes: a torrent whose pieces fit is written
// whole once verified, as before, and one whose pieces do not is written
// block by block; both end with the same files and the same pieces had.
#[test]
fn a_fitting_piece_is_held_whole_as_before_and_ends_as_a_block_written_one_does() {
    let torrent = noise(160_000, 4);
    let fitting = shim_over(tempfile::tempdir().unwrap(), unbounded(), None);
    let budget = accountant(BUDGET);
    let oversized = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    assert!(fitting.storage.writes_to_memory());
    assert!(!oversized.storage.writes_to_memory());
    for piece in 0..3 {
        fitting.write(piece, &torrent);
        // Held whole: nothing of it is on disk before it verified.
        if piece == 0 {
            assert!(!fitting.dir.path().join("a.bin").exists());
        }
        fitting.storage.on_piece_completed(fitting.index(piece)).unwrap();
        oversized.write_blocks(piece, &torrent, u64::MAX, None);
        oversized
            .storage
            .on_piece_completed(oversized.index(piece))
            .unwrap();
    }
    assert_eq!(fitting.files(), oversized.files());
    assert_eq!(fitting.files()[0], torrent[..100_000]);
    assert_eq!(fitting.store.count(), 3);
    assert_eq!(oversized.store.count(), 3);
    assert_eq!(fitting.store.file_have(), oversized.store.file_have());
    // The fitting torrent held a whole piece; the other one block.
    assert_eq!(fitting.storage.resident_peak(), PIECE);
    assert_eq!(oversized.storage.resident_peak(), BLOCK);
}

// A block that arrived wrong is on disk, so the hash from disk fails: the
// piece is not had, its room goes back, and it is fetched again, every
// block written anew, and had once it verifies. The store never had it in
// between, so no have was owed for it.
#[test]
fn a_bad_block_fails_the_hash_from_disk_and_the_piece_is_fetched_again() {
    let budget = accountant(BUDGET);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    let torrent = noise(160_000, 2);
    shim.write_blocks(0, &torrent, u64::MAX, Some(2));
    let read = shim.read(0);
    assert_ne!(read, torrent[..65_536]);
    assert_eq!(read[..32_768], torrent[..32_768]);

    shim.storage.on_piece_hash_failed(shim.index(0)).unwrap();
    assert!(!shim.store.has(0));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);

    shim.write_blocks(0, &torrent, u64::MAX, None);
    assert!(!shim.store.has(0));
    assert_eq!(shim.read(0), torrent[..65_536]);
    shim.storage.on_piece_completed(shim.index(0)).unwrap();
    assert!(shim.store.has(0));
    assert!(shim.storage.resident_peak() <= BUDGET);
}

// A restart in the middle of a piece: the blocks written are on disk, but
// the piece was never had, so it is fetched again. The blocks moved the
// time of the file they went to, so the pieces kept in that file are
// checked again at the restart, as any file written since its time was
// taken is; a file the piece did not reach keeps its pieces. The check
// finds them, and the piece fetched again is had.
#[test]
fn a_restart_mid_piece_fetches_it_again_and_checks_its_files_kept_pieces() {
    let budget = accountant(BUDGET);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    let torrent = noise(160_000, 6);
    for piece in [0, 2] {
        shim.write_blocks(piece, &torrent, u64::MAX, None);
        shim.storage.on_piece_completed(shim.index(piece)).unwrap();
    }
    let stamps: std::collections::HashMap<u32, i64> = shim
        .store
        .stamping()
        .unwrap()
        .look()
        .into_iter()
        .map(|(file, time)| (file, time.unwrap()))
        .collect();
    let kept = shim.store.kept();
    // Two blocks of piece 1, all in a.bin, and then the restart.
    shim.write_blocks(1, &torrent, 2, None);
    let Shim {
        store,
        storage,
        dir,
        ..
    } = shim;
    drop(storage);
    store.forget_now();
    drop(store);
    // The writes moved a.bin's time; moved past any clock's grain here.
    let a = dir.path().join("a.bin");
    let was = std::fs::metadata(&a).unwrap().modified().unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&a)
        .unwrap()
        .set_modified(was + std::time::Duration::from_secs(5))
        .unwrap();

    let again = shim_over(dir, holds(&budget), Some(&kept));
    again.store.set_stamps(stamps);
    assert_eq!(again.store.drop_unbacked(), Some(0));
    let checkpoint = again.store.initial_checkpoint(false).unwrap();
    assert_eq!(
        checkpoint.have.iter_ones().collect::<Vec<_>>(),
        vec![2],
        "piece 0 is checked again with a.bin, piece 1 was never had"
    );
    assert!(again.store.found(0).unwrap());
    again.write_blocks(1, &torrent, u64::MAX, None);
    again.storage.on_piece_completed(again.index(1)).unwrap();
    assert_eq!(again.store.count(), 3);
    assert_eq!(again.files()[0], torrent[..100_000]);
    assert_eq!(again.files()[1], torrent[100_000..]);
}

// The write path is chosen when the storage is made, from the piece length
// and the resident budget alone, so the same torrent under the same budget
// takes the same path after a restart; nothing about it is stored.
#[test]
fn the_write_path_is_the_same_after_a_restart() {
    let layout = two_files();
    let dir = tempfile::tempdir().unwrap();
    let path = |budget: u64| {
        let store = store_in(&layout, dir.path(), None);
        let factory = PieceStorageFactory::new(
            Arc::clone(&layout),
            store as Arc<dyn Pieces>,
            holds(&accountant(budget)),
        );
        factory.writes_blocks()
    };
    assert!(path(BUDGET));
    assert!(path(BUDGET), "again after a restart");
    assert!(!path(PIECE));
    assert!(!path(u64::MAX));
}

// A torrent of pieces over the budget fetched from a peer: every block is
// written as it arrives, every piece hashed from disk and had, the files
// end as the seeder's, and the storage never held more
// than its budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_torrent_downloads_from_a_peer_within_its_budget() {
    let seeded = seeded_sized(5, PIECE as u32, [100_000, 150_000, 40_000]).await;
    let layout = &seeded.layout;
    let fetch_dir = tempfile::tempdir().unwrap();
    let disk = disk_off_runtime();
    let store = store_on(&disk, 1, layout, &fetch_dir.path().join("job"));
    let (watched, mut in_hand) = watched(Arc::clone(&store));
    let budget = accountant(BUDGET);
    let factory = PieceStorageFactory::new(
        Arc::clone(layout),
        Arc::clone(&watched) as Arc<dyn Pieces>,
        holds(&budget),
    );
    assert!(factory.writes_blocks());
    let peak = factory.resident_peak();
    let leecher = fetch(&seeded, fetch_dir.path(), None, factory.boxed_factory()).await;
    in_hand.wait_for(|whole| *whole).await.unwrap();
    for (name, original) in &seeded.originals {
        assert_eq!(
            &std::fs::read(fetch_dir.path().join("job").join(name)).unwrap(),
            original
        );
    }
    // No piece was handed over whole.
    assert_eq!(*watched.handed.borrow(), 0);
    let peak = peak.peak.load(Ordering::Acquire);
    assert!((BLOCK..=BUDGET).contains(&peak), "{peak}");
    leecher.stop().await;
    seeded.seeder.stop().await;
}
