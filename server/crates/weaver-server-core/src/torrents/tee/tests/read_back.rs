// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An archive torrent whose pieces are larger than the resident budget: its
// pieces are written block by block, and its archive shares read back from
// disk for unpacking, through scratch memory, or in smaller reads when
// scratch cannot be had, after a piece verifies and at a restart.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use weaver_graph::NodeId;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant, HoldsNamespace};
use weaver_torrent::core::lengths::Lengths;
use weaver_torrent::storage::TorrentStorage;

use super::super::{READ_BACK_BYTES, Reading, Word};
use super::{file, parts, storage};
use crate::operations::disk::{Capacity, CapacityReader, CapacityReading};
use crate::pipeline::graph::engine::RunMail;
use crate::scratch::{CEILING_BYTES, Scratch, ScratchLedger};
use crate::torrents::files::Files;
use crate::torrents::intake::Holds;
use crate::torrents::layout::PieceLayout;
use crate::torrents::storage::{PieceStorage, Pieces};
use crate::torrents::store::{Kept, TorrentStore};
use crate::torrents::tee::{Plan, Tee};

const MIB: u64 = 1 << 20;
const PIECE: u64 = 2 * MIB;
// Room for one read-back article and a half: no piece fits whole.
const BUDGET: u64 = 3 * MIB / 2;
const BLOCK: u64 = 16 << 10;

// Bytes no compressor shrinks, so the archive is as large as its member.
fn noise(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

// A gzipped tar of one member, `big.bin`.
fn tar_gz(member: &[u8]) -> Vec<u8> {
    let mut tar = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(member.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "big.bin", member).unwrap();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut gzip, &tar.into_inner().unwrap()).unwrap();
    gzip.finish().unwrap()
}

fn holds(limit: u64) -> Holds {
    Holds {
        accountant: Arc::new(HoldsAccountant::new(HoldBudgets {
            resident_limit: limit,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        })),
        namespace: HoldsNamespace {
            scope: 1,
            node: NodeId::ROOT,
        },
    }
}

// Scratch that refuses every lease: its disk would go below the free space
// it keeps.
fn refusing_scratch(work: &Path) -> Scratch {
    let free = CapacityReader::from_fn(|| {
        Capacity::Known(CapacityReading {
            available_bytes: 1 << 30,
            total_bytes: 1 << 40,
            sampled_at: std::time::Instant::now(),
            stale: false,
        })
    });
    let keeps_free = Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 1 << 40,
    }));
    Scratch::new(
        work.to_path_buf(),
        free,
        ScratchLedger::default(),
        keeps_free,
        CEILING_BYTES,
    )
}

struct Rig {
    dir: tempfile::TempDir,
    mail: RunMail,
    layout: Arc<PieceLayout>,
    archive: Vec<u8>,
    member: Vec<u8>,
    tee: Arc<Tee>,
    scratch: Arc<Scratch>,
    peaks: Arc<super::super::Peaks>,
    holds: Holds,
}

// A tee over a gzipped tar in pieces of `PIECE`, which reads its pieces
// back, through scratch, or through scratch that refuses when `refused`.
fn rig(refused: bool) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let member = noise((3 * MIB + 12_345) as usize, 7);
    let archive = tar_gz(&member);
    let layout =
        Arc::new(PieceLayout::new(PIECE, vec![file("set.tar.gz", archive.len() as u64)]).unwrap());
    assert!(layout.piece_count() >= 2);
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let scratch = Arc::new(if refused {
        refusing_scratch(&work)
    } else {
        storage(&work, &mail, CapacityReader::unknown()).scratch(1)
    });
    let holds = holds(BUDGET);
    let reading = Reading::new(Arc::clone(&scratch), holds.clone());
    let peaks = Arc::clone(&reading.peaks);
    let mut parts = parts(&layout, &work, &mail, Arc::default());
    parts.plan = Plan::of(&layout).unwrap().reading_back(true);
    parts.reading = Some(reading);
    let tee = Tee::fresh(parts, storage(&work, &mail, CapacityReader::unknown()));
    Rig {
        dir,
        mail,
        layout,
        archive,
        member,
        tee,
        scratch,
        peaks,
        holds,
    }
}

impl Rig {
    // The torrent's store over its files, with the pieces it kept, writing
    // through writers made off the runtime, so what follows a piece's
    // writes runs where the piece completed.
    fn store(&self, kept: Option<&Kept>) -> Arc<TorrentStore> {
        let writes = std::thread::spawn(crate::torrents::writes::Writes::for_test)
            .join()
            .unwrap();
        TorrentStore::new(
            Arc::clone(&self.layout),
            Files::new(&self.layout, self.files()).unwrap(),
            kept,
            Some(Arc::clone(&self.tee)),
            writes,
        )
        .shared()
    }

    fn files(&self) -> PathBuf {
        self.dir.path().join("files")
    }

    // Every piece but `skipped` fetched as the session fetches one too large
    // to hold: block by block, then verified.
    fn fetch(&self, store: &Arc<TorrentStore>, skipped: Option<u32>) -> PieceStorage {
        let storage = PieceStorage::new(
            Arc::clone(&self.layout),
            Arc::clone(store) as Arc<dyn Pieces>,
            self.holds.clone(),
        );
        assert!(!storage.writes_to_memory());
        let lengths = Lengths::new(self.layout.total(), PIECE as u32).unwrap();
        for piece in (0..self.layout.piece_count()).filter(|piece| Some(*piece) != skipped) {
            let index = lengths.try_validate_piece_index(piece).unwrap();
            assert!(storage.reserve_piece(index, &Arc::default()));
            let start = self.layout.piece_range(piece).start;
            for span in self.layout.spans(piece) {
                let end = span.at + span.range.len();
                let mut at = span.at;
                while at < end {
                    let stop = end.min((at / BLOCK + 1) * BLOCK);
                    let bytes = &self.archive[(start + at) as usize..(start + stop) as usize];
                    storage
                        .pwrite_all(span.file, span.range.start + (at - span.at), bytes)
                        .unwrap();
                    at = stop;
                }
            }
            storage.on_piece_completed(index).unwrap();
            // Read back where it completed, and taken.
            assert!(!self.tee.wants(piece), "piece {piece} went to the job");
        }
        storage
    }

    // The job's runs heard until it is done, then what it unpacked.
    async fn unpacked(&mut self) -> Vec<u8> {
        while !self.tee.done() {
            let said = self.mail.recv().await.unwrap();
            self.tee.take(Word::Said(said, 1), (false, false));
        }
        let mut found = None;
        let mut dirs = vec![self.tee.work().to_path_buf()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.file_name().is_some_and(|name| name == "big.bin") {
                    found = Some(std::fs::read(path).unwrap());
                }
            }
        }
        found.expect("big.bin unpacked")
    }

    fn scratch_peak(&self) -> u64 {
        self.peaks.scratch_peak.load(Ordering::Acquire)
    }

    fn resident_peak(&self) -> u64 {
        self.peaks.resident_peak.load(Ordering::Acquire)
    }

    fn fallbacks(&self) -> u64 {
        self.peaks.fallbacks.load(Ordering::Acquire)
    }
}

// A share of a piece is cut into articles of `READ_BACK_BYTES`, the same
// layout every time, and a torrent that holds its pieces whole keeps one
// article a share.
#[test]
fn a_read_back_torrents_shares_are_cut_into_articles_of_one_layout() {
    let layout = PieceLayout::new(
        PIECE,
        vec![file("a.rar", 3 * MIB + 5), file("a.r00", 2 * MIB)],
    )
    .unwrap();
    let whole = Plan::of(&layout).unwrap();
    let cut = whole.clone().reading_back(true);
    assert!(!whole.reads_back() && cut.reads_back());
    assert_eq!(whole.clone().reading_back(false), whole);
    assert_eq!(Plan::of(&layout).unwrap().reading_back(true), cut);
    let lengths = |plan: &Plan| -> Vec<Vec<(u32, u32)>> {
        plan.root_files(&layout)
            .into_iter()
            .map(|file| file.articles)
            .collect()
    };
    // Held whole: one article a piece's share, numbered by piece.
    assert_eq!(
        lengths(&whole),
        [
            vec![(0, 2 << 20), (1, (1 << 20) + 5)],
            vec![(0, (1 << 20) - 5), (1, (1 << 20) + 5)],
        ]
    );
    // Read back: every share in articles of 1 MiB, numbered on.
    assert_eq!(READ_BACK_BYTES, MIB);
    assert_eq!(
        lengths(&cut),
        [
            vec![(0, 1 << 20), (1, 1 << 20), (2, 1 << 20), (3, 5)],
            vec![(0, (1 << 20) - 5), (1, 1 << 20), (2, 5)],
        ]
    );
}

// The pieces read back through one scratch lease each, which the graph
// holds until it let go of the last byte of it: the archive unpacks whole,
// no lease is left, scratch never held more than one piece, and the read-
// backs took no resident room beside each piece's own block room.
#[tokio::test]
async fn an_oversized_archive_piece_reads_back_through_scratch_and_unpacks() {
    let mut rig = rig(false);
    let store = rig.store(None);
    let storage = rig.fetch(&store, None);
    assert_eq!(rig.unpacked().await, rig.member);
    assert_eq!(rig.fallbacks(), 0);
    let peak = rig.scratch_peak();
    assert!((1..=PIECE).contains(&peak), "{peak}");
    assert_eq!(rig.scratch.live(), (0, 0));
    assert_eq!(rig.resident_peak(), 0);
    assert!(storage.resident_peak() <= BUDGET);
    assert_eq!(storage.resident_peak(), READ_BACK_BYTES);
    assert_eq!(
        rig.holds
            .accountant
            .namespace_usage(rig.holds.namespace)
            .resident,
        0
    );
    rig.tee.forget();
}

// With scratch refused, every piece is read back an article at a time in
// resident memory: in the piece's own room, or room the budget grants
// first, never past it. The archive unpacks whole all the same.
#[tokio::test]
async fn an_oversized_archive_piece_reads_back_in_smaller_reads_when_scratch_is_refused() {
    let mut rig = rig(true);
    let store = rig.store(None);
    let storage = rig.fetch(&store, None);
    assert_eq!(rig.unpacked().await, rig.member);
    assert_eq!(rig.fallbacks(), u64::from(rig.layout.piece_count()));
    assert_eq!(rig.scratch_peak(), 0);
    assert!(rig.resident_peak() <= BUDGET);
    assert!(storage.resident_peak() <= BUDGET);
    assert_eq!(
        rig.holds
            .accountant
            .namespace_usage(rig.holds.namespace)
            .resident,
        0
    );
    rig.tee.forget();
}

// A restart: the torrent kept every piece, but one never reached the job.
// That one is read back from the files at the restart by the same rule,
// through scratch, or in smaller reads when scratch is refused, and the
// archive unpacks whole.
#[tokio::test]
async fn a_restart_reads_a_lacking_piece_back_by_the_same_rule() {
    for refused in [false, true] {
        let mut rig = rig(refused);
        let lacked = rig.layout.piece_count() - 1;
        // Its bytes are in the files, put there before the restart.
        std::fs::create_dir_all(rig.files()).unwrap();
        std::fs::write(rig.files().join("set.tar.gz"), &rig.archive).unwrap();
        let store = rig.store(None);
        let storage = rig.fetch(&store, Some(lacked));
        drop(storage);
        assert!(rig.tee.wants(lacked));
        let fallbacks = rig.fallbacks();

        let pieces = rig.layout.piece_count() as usize;
        let mut bits = bitvec::vec::BitVec::<u8, bitvec::order::Msb0>::repeat(true, pieces);
        bits.set_uninitialized(false);
        let kept = Kept {
            bits: bits.into_vec(),
            checking: false,
        };
        let again = rig.store(Some(&kept));
        let replayed = again.replay_had();
        assert_eq!(replayed.taken, 1, "refused {refused}");
        assert!(replayed.unread.is_none());
        assert_eq!(rig.unpacked().await, rig.member, "refused {refused}");
        assert_eq!(rig.fallbacks() - fallbacks, u64::from(refused));
        assert!(rig.resident_peak() <= BUDGET);
        assert!(rig.scratch_peak() <= PIECE);
        rig.tee.forget();
    }
}

// A read-back the budget has no room for waits for room, not for a
// restart: with scratch refused and the budget held by a piece in flight,
// the lacking piece's read-back is refused and waits; the piece's room let
// go rings it, and it is read back and the archive unpacks, in the same
// run. The budget is never passed.
#[tokio::test]
async fn a_read_back_refused_for_room_goes_again_once_room_returns() {
    let mut rig = rig(true);
    let lacked = rig.layout.piece_count() - 1;
    std::fs::create_dir_all(rig.files()).unwrap();
    std::fs::write(rig.files().join("set.tar.gz"), &rig.archive).unwrap();
    let store = rig.store(None);
    drop(rig.fetch(&store, Some(lacked)));
    assert!(rig.tee.wants(lacked));

    let pieces = rig.layout.piece_count() as usize;
    let mut bits = bitvec::vec::BitVec::<u8, bitvec::order::Msb0>::repeat(true, pieces);
    bits.set_uninitialized(false);
    let kept = Kept {
        bits: bits.into_vec(),
        checking: false,
    };
    let again = rig.store(Some(&kept));
    // A piece in flight holds the room the read-back would need.
    let storage = PieceStorage::new(
        Arc::clone(&rig.layout),
        Arc::clone(&again) as Arc<dyn Pieces>,
        rig.holds.clone(),
    );
    let lengths = Lengths::new(rig.layout.total(), PIECE as u32).unwrap();
    let flying = lengths.try_validate_piece_index(0).unwrap();
    assert!(storage.reserve_piece(flying, &Arc::default()));
    let held = rig
        .holds
        .accountant
        .namespace_usage(rig.holds.namespace)
        .resident;
    assert!(held + READ_BACK_BYTES > BUDGET, "{held}");
    let resident_before = rig.resident_peak();

    let replayed = again.replay_had();
    assert_eq!(replayed.taken, 0);
    assert!(replayed.unread.is_none());
    assert!(rig.tee.waits_for_room());
    assert!(rig.tee.wants(lacked));
    assert_eq!(rig.resident_peak(), resident_before);

    // The piece fails its hash: its room goes back, and rings the wait.
    storage.on_piece_hash_failed(flying).unwrap();
    assert_eq!(rig.unpacked().await, rig.member);
    assert!(!rig.tee.wants(lacked));
    assert!(!rig.tee.waits_for_room());
    assert!(rig.resident_peak() <= BUDGET);
    assert!(storage.resident_peak() <= BUDGET);
    assert_eq!(
        rig.holds
            .accountant
            .namespace_usage(rig.holds.namespace)
            .resident,
        0
    );
    rig.tee.forget();
}
