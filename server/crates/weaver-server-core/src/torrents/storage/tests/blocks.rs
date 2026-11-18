// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Pieces larger than the resident budget: written block by block as they
// arrive, hashed from disk, had once the hash passed.

use std::io::IoSlice;
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
    Arc::new(PieceLayout::new(PIECE, vec![file("a.bin", 100_000), file("b.bin", 60_000)]).unwrap())
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
        fitting
            .storage
            .on_piece_completed(fitting.index(piece))
            .unwrap();
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

// Room for one whole piece and one block.
const PIECE_AND_BLOCK: u64 = PIECE + BLOCK;

// A piece whose owner left part way is given back: the blocks it holds in
// memory go to its files, and its whole-piece room goes back, all of it,
// which another piece's first block then takes. The rest of it arrives
// block by block, each block holding room for its write alone (taken by the
// session, not here), it is checked from its files, and it is had.
#[tokio::test]
async fn a_piece_given_back_part_way_is_written_out_and_frees_its_room_and_verifies() {
    let budget = accountant(PIECE_AND_BLOCK);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    let torrent = noise(160_000, 5);
    assert!(shim.storage.writes_to_memory());
    shim.write_blocks(1, &torrent, 2, None);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
    assert!(!shim.storage.reserve_piece(shim.index(0), &shim.room));
    assert!(!shim.dir.path().join("a.bin").exists());

    let rung = shim.room.notified();
    shim.storage.on_piece_requeued(shim.index(1));
    rung.await;
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(!shim.storage.writes_piece_to_memory(shim.index(1)));
    let a = std::fs::read(shim.dir.path().join("a.bin")).unwrap();
    assert_eq!(&a[65_536..98_304], &torrent[65_536..98_304]);
    assert_eq!(shim.store.count(), 0);
    assert!(shim.storage.reserve_piece(shim.index(0), &shim.room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);

    shim.write_blocks(1, &torrent, u64::MAX, None);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
    assert!(!shim.storage.hash_held_piece(shim.index(1), &mut |_| {}));
    assert_eq!(shim.read(1), torrent[65_536..131_072]);
    shim.storage.on_piece_completed(shim.index(1)).unwrap();
    assert!(shim.store.has(1));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
    assert!(shim.storage.resident_peak() <= PIECE_AND_BLOCK);
}

// The store, its disk full for every block written alone.
struct FullForBlocks {
    store: Arc<TorrentStore>,
}

impl Pieces for FullForBlocks {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        self.store.verified(piece, held)
    }

    fn write_block(&self, _: u32, _: usize, _: u64, _: u64, _: &[u8]) -> anyhow::Result<()> {
        Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into())
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
        self.store.read(file, offset, out)
    }

    fn grow(&self, file: usize, length: u64) -> std::io::Result<()> {
        self.store.grow(file, length)
    }
}

// A piece given back whose blocks fail to reach its files frees its room
// all the same, and says so before it is checked: it is refused, with the
// disk's error, for the session to fetch it again blaming no one. Its
// later blocks go to its files too, never to memory on the runtime.
#[test]
fn a_piece_given_back_whose_blocks_fail_to_write_is_refused_and_frees_its_room() {
    let budget = accountant(PIECE_AND_BLOCK);
    let layout = two_files();
    let dir = tempfile::tempdir().unwrap();
    let store = store_in(&layout, dir.path(), None);
    let full = Arc::new(FullForBlocks {
        store: Arc::clone(&store),
    });
    let shim = Shim {
        store,
        storage: PieceStorage::new(Arc::clone(&layout), full as Arc<dyn Pieces>, holds(&budget)),
        layout,
        dir,
        room: Arc::default(),
    };
    let torrent = noise(160_000, 7);
    shim.write_blocks(1, &torrent, 2, None);
    shim.storage.piece_written(shim.index(1)).unwrap();

    shim.storage.on_piece_requeued(shim.index(1));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    let error = shim.storage.piece_written(shim.index(1)).unwrap_err();
    assert!(weaver_torrent::storage::write_refused(&error), "{error:#}");
    // A block landing on the runtime is left for a thread that may wait.
    let block = &torrent[98_304..100_000];
    let on_runtime = shim
        .storage
        .pwrite_held(0, 98_304, [IoSlice::new(block), IoSlice::new(&[])])
        .unwrap();
    assert!(!on_runtime);

    shim.storage.on_piece_hash_failed(shim.index(1)).unwrap();
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(!shim.store.has(1));
}

// A piece given back whole stays in memory, checked from there as before.
#[test]
fn a_piece_given_back_whole_stays_in_memory() {
    let budget = accountant(PIECE_AND_BLOCK);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    let torrent = noise(160_000, 6);
    shim.write_blocks(0, &torrent, u64::MAX, None);
    shim.storage.on_piece_requeued(shim.index(0));
    assert!(shim.storage.writes_piece_to_memory(shim.index(0)));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
    assert!(!shim.dir.path().join("a.bin").exists());
    shim.storage.on_piece_completed(shim.index(0)).unwrap();
    assert!(shim.store.has(0));
}

// A whole piece is hashed under its own lock. Given back as it is hashed,
// it is left in memory without its lock being waited on, and the map of
// pieces in flight stays free for every other piece's blocks.
#[test]
fn a_piece_given_back_as_it_is_hashed_holds_up_no_other_piece() {
    let budget = accountant(PIECE_AND_BLOCK);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    let torrent = noise(160_000, 3);
    shim.write_blocks(0, &torrent, u64::MAX, None);
    let writing = match shim.storage.inner.flight_read().get(&0) {
        Some(crate::torrents::storage::Flight::Writing(writing, _)) => Arc::clone(writing),
        _ => panic!("piece 0 is held in memory"),
    };
    let hashing = writing.read();
    shim.storage.on_piece_requeued(shim.index(0));
    assert!(shim.storage.inner.flight.try_write().is_ok());
    drop(hashing);
    assert!(shim.storage.writes_piece_to_memory(shim.index(0)));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
    shim.storage.on_piece_completed(shim.index(0)).unwrap();
    assert!(shim.store.has(0));
}

// A block of a piece no peer owns holds no room for its piece, which is
// written block by block, each block holding room for its write alone; a
// whole piece's room is left for an owner.
#[test]
fn an_unowned_pieces_first_block_holds_no_room_for_its_piece() {
    let budget = accountant(PIECE_AND_BLOCK);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    assert!(
        shim.storage
            .reserve_unowned_piece(shim.index(1), &shim.room)
    );
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(!shim.storage.writes_piece_to_memory(shim.index(1)));
    // Asked again, as each of its blocks asks, it holds no more.
    assert!(
        shim.storage
            .reserve_unowned_piece(shim.index(1), &shim.room)
    );
    assert!(shim.storage.reserve_piece(shim.index(1), &shim.room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(shim.storage.reserve_piece(shim.index(0), &shim.room));
    assert!(shim.storage.writes_piece_to_memory(shim.index(0)));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
    // A block of the piece given no room holds room for its write alone.
    let room = shim.storage.hold_block(BLOCK, &shim.room);
    assert!(matches!(room, weaver_torrent::storage::BlockRoom::Held(_)));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE_AND_BLOCK);
    assert!(matches!(
        shim.storage.hold_block(BLOCK, &shim.room),
        weaver_torrent::storage::BlockRoom::Refused
    ));
    drop(room);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
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

const OTHER: HoldsNamespace = HoldsNamespace {
    scope: 2,
    node: NodeId::ROOT,
};

// A session refused room another job of the budget holds is rung when that
// job lets it go, not only when a piece of its own does, and its piece is
// granted then.
#[tokio::test]
async fn room_another_holder_lets_go_rings_a_session_refused_it() {
    let budget = accountant(PIECE);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    assert_eq!(
        budget.reserve_resident(OTHER, BLOCK),
        weaver_graph::holds::Reserve::Granted
    );
    assert!(!shim.storage.reserve_piece(shim.index(0), &shim.room));
    let rung = shim.room.notified();
    budget.release_resident(OTHER, BLOCK);
    rung.await;
    assert!(shim.storage.room_for(PIECE, &shim.room));
    assert!(shim.storage.reserve_piece(shim.index(0), &shim.room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
}

const THIRD: HoldsNamespace = HoldsNamespace {
    scope: 3,
    node: NodeId::ROOT,
};

// Whether `rung` is ready now, without waiting for it.
fn rung_now(rung: std::pin::Pin<&mut tokio::sync::futures::Notified<'_>>) -> bool {
    use std::future::Future;
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    rung.poll(&mut cx).is_ready()
}

// A session refused room keeps a wake behind the refusal, whatever happens
// between its first ask and its last: here the holder that filled the
// budget lets go and a third takes the room before the last ask. When the
// third lets go, the session is rung.
#[tokio::test]
async fn room_taken_between_two_asks_still_rings_the_session_refused() {
    let budget = accountant(PIECE);
    let shim = shim_over(tempfile::tempdir().unwrap(), holds(&budget), None);
    assert_eq!(
        budget.reserve_resident(OTHER, PIECE),
        weaver_graph::holds::Reserve::Granted
    );
    let third = Arc::clone(&budget);
    shim.storage.on_refusal(move || {
        third.release_resident(OTHER, PIECE);
        assert_eq!(
            third.reserve_resident(THIRD, PIECE),
            weaver_graph::holds::Reserve::Granted
        );
    });
    assert!(!shim.storage.reserve_piece(shim.index(0), &shim.room));
    assert_eq!(budget.namespace_usage(THIRD).resident, PIECE);
    let rung = shim.room.notified();
    let mut rung = std::pin::pin!(rung);
    assert!(!rung_now(rung.as_mut()), "nothing gave room back yet");
    budget.release_resident(THIRD, PIECE);
    assert!(rung_now(rung.as_mut()), "the refusal is rung");
    assert!(shim.storage.reserve_piece(shim.index(0), &shim.room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, PIECE);
}

fn framed(out: &mut Vec<u8>, id: u8, payload: &[&[u8]]) {
    let length = 1 + payload.iter().map(|part| part.len()).sum::<usize>();
    out.extend_from_slice(&u32::try_from(length).unwrap().to_be_bytes());
    out.push(id);
    for part in payload {
        out.extend_from_slice(part);
    }
}

// A seeder of `content` that has every piece and answers each batch of
// requests out of order: the blocks of the pieces asked for are interleaved,
// the piece asked for last first. While one piece alone is asked for, the
// last of its blocks asked for is held back until another piece is asked
// for too, or every piece has been: a block of a second piece then always
// arrives while the first is still open.
async fn scrambled_seed(
    listener: tokio::net::TcpListener,
    info_hash: [u8; 20],
    content: Vec<u8>,
    pieces: u32,
) {
    use std::collections::VecDeque;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (stream, _) = listener.accept().await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (read, mut write) = stream.into_split();
    let mut read = tokio::io::BufReader::new(read);
    let mut theirs = [0u8; 68];
    read.read_exact(&mut theirs).await.unwrap();
    assert_eq!(&theirs[28..48], &info_hash);
    let mut out = vec![19];
    out.extend_from_slice(b"BitTorrent protocol");
    out.extend_from_slice(&[0; 8]);
    out.extend_from_slice(&info_hash);
    out.extend_from_slice(b"-WB0001-scrambled-01");
    let mut bits = vec![0xffu8; pieces.div_ceil(8) as usize];
    let spare = u32::try_from(bits.len()).unwrap() * 8 - pieces;
    if spare > 0 {
        *bits.last_mut().unwrap() = 0xff << spare;
    }
    framed(&mut out, 5, &[&bits]);
    framed(&mut out, 1, &[]);
    write.write_all(&out).await.unwrap();

    // Pieces in the order first asked for, and the requests unanswered.
    let mut seen: Vec<u32> = Vec::new();
    let mut pending: Vec<(u32, u32, u32)> = Vec::new();
    let mut payload = Vec::new();
    loop {
        let Ok(length) = read.read_u32().await else {
            return;
        };
        if length > 0 {
            let Ok(id) = read.read_u8().await else {
                return;
            };
            payload.resize(length as usize - 1, 0);
            if read.read_exact(&mut payload).await.is_err() {
                return;
            }
            let field = |at: usize| u32::from_be_bytes(payload[at..at + 4].try_into().unwrap());
            match id {
                6 => {
                    let asked = (field(0), field(4), field(8));
                    if !seen.contains(&asked.0) {
                        seen.push(asked.0);
                    }
                    pending.push(asked);
                }
                8 => {
                    let cancelled = (field(0), field(4), field(8));
                    pending.retain(|asked| *asked != cancelled);
                }
                _ => {}
            }
        }
        if !read.buffer().is_empty() || pending.is_empty() {
            continue;
        }
        let mut queues: Vec<VecDeque<(u32, u32, u32)>> = seen
            .iter()
            .rev()
            .map(|piece| {
                pending
                    .iter()
                    .filter(|asked| asked.0 == *piece)
                    .copied()
                    .collect()
            })
            .filter(|queue: &VecDeque<_>| !queue.is_empty())
            .collect();
        let mut kept = Vec::new();
        if queues.len() == 1 && u32::try_from(seen.len()).unwrap() < pieces {
            kept.extend(queues[0].pop_back());
        }
        out.clear();
        while queues.iter().any(|queue| !queue.is_empty()) {
            for queue in &mut queues {
                if let Some((piece, begin, length)) = queue.pop_front() {
                    let at = (u64::from(piece) * PIECE + u64::from(begin)) as usize;
                    framed(
                        &mut out,
                        7,
                        &[
                            &piece.to_be_bytes(),
                            &begin.to_be_bytes(),
                            &content[at..at + length as usize],
                        ],
                    );
                }
            }
        }
        pending = kept;
        if write.write_all(&out).await.is_err() {
            return;
        }
    }
}

// The jam end to end, on the server's storage with room for exactly one
// piece: a peer that sends a second piece's blocks while the first is open
// stops the session for room, which gives the first back, writes its blocks
// out as they come, and finishes every piece; the files end as the seeder's
// and the budget is whole again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_out_of_order_peer_finishes_every_piece_with_room_for_one() {
    let seeded = seeded_sized(8, PIECE as u32, [100_000, 150_000, 40_000]).await;
    seeded.seeder.stop().await;
    let layout = &seeded.layout;
    let content: Vec<u8> = seeded
        .originals
        .iter()
        .flat_map(|(_, bytes)| bytes.iter().copied())
        .collect();
    let fetch_dir = tempfile::tempdir().unwrap();
    let disk = disk_off_runtime();
    let store = store_on(&disk, 1, layout, &fetch_dir.path().join("job"));
    let (watched, mut in_hand) = watched(Arc::clone(&store));
    let budget = accountant(PIECE);
    let factory = PieceStorageFactory::new(
        Arc::clone(layout),
        Arc::clone(&watched) as Arc<dyn Pieces>,
        holds(&budget),
    );
    assert!(!factory.writes_blocks(), "a piece fits the budget whole");

    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let peer = listener.local_addr().unwrap();
    let info_hash = torrent_from_bytes(&seeded.bytes).unwrap().info_hash.0;
    let seeder = tokio::spawn(scrambled_seed(
        listener,
        info_hash,
        content,
        layout.piece_count(),
    ));
    let leecher = session(&fetch_dir.path().join("session"), None).await;
    leecher
        .add_torrent(
            AddTorrent::from_bytes(seeded.bytes.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(
                    fetch_dir
                        .path()
                        .join("unused")
                        .to_string_lossy()
                        .into_owned(),
                ),
                overwrite: true,
                initial_peers: Some(vec![peer]),
                storage_factory: Some(factory.boxed_factory()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    in_hand.wait_for(|whole| *whole).await.unwrap();
    for (name, original) in &seeded.originals {
        assert_eq!(
            &std::fs::read(fetch_dir.path().join("job").join(name)).unwrap(),
            original
        );
    }
    let waits = leecher.stats_snapshot().swarm.request_events
        [weaver_torrent::session_stats::swarm::RequestEvent::RoomWait as usize];
    assert!(waits > 0, "the peer's order stopped the session for room");
    leecher.stop().await;
    seeder.abort();
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}
