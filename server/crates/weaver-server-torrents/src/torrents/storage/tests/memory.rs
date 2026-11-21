// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The memory of pieces in flight, as the OS backs it: a piece granted holds
// its room from the grant, and memory only for the blocks that arrived.

use std::sync::Arc;

use tokio::sync::Notify;
use weaver_torrent::core::constants::CHUNK_SIZE;
use weaver_torrent::core::lengths::{Lengths, ValidPieceIndex};
use weaver_torrent::storage::TorrentStorage;

use super::{NAMESPACE, Recorder, accountant, file, holds, noise, store_in};
use crate::torrents::layout::PieceLayout;
use crate::torrents::storage::{PieceStorage, Pieces, resident_of};

// The field's pieces: 256 KiB, sixteen blocks.
const PIECE: u64 = 256 * 1024;
const BLOCK: u64 = CHUNK_SIZE as u64;

// What the OS backs one arrived block with: whole pages.
fn block_memory() -> u64 {
    let page = resident_of(&noise(1, 0)[..]).max(1);
    BLOCK.div_ceil(page) * page
}

fn pieces(count: u32) -> (Arc<PieceLayout>, Vec<ValidPieceIndex>) {
    let total = PIECE * u64::from(count);
    let layout = Arc::new(PieceLayout::new(PIECE, vec![file("a", total)]).unwrap());
    let lengths = Lengths::new(total, PIECE as u32).unwrap();
    let indexes = (0..count)
        .map(|piece| lengths.try_validate_piece_index(piece).unwrap())
        .collect();
    (layout, indexes)
}

fn block_of(storage: &PieceStorage, piece: u32, block: u64, seed: u8) {
    let at = u64::from(piece) * PIECE + block * BLOCK;
    storage
        .pwrite_all(0, at, &noise(BLOCK as usize, seed))
        .unwrap();
}

// Many pieces granted to slow peers, one block of each arrived: each holds
// its whole room against the budget from the grant, as before, and memory
// for the one block alone. A grant takes no memory at all.
#[test]
fn pieces_granted_hold_memory_only_for_the_blocks_that_arrived() {
    let (layout, indexes) = pieces(600);
    let budget = accountant(u64::MAX);
    let storage = PieceStorage::new(
        layout,
        Arc::new(Recorder::default()) as Arc<dyn Pieces>,
        holds(&budget),
    );
    let room = Arc::new(Notify::new());
    for &piece in &indexes {
        assert!(storage.reserve_piece(piece, &room));
    }
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 600 * PIECE);
    assert_eq!(storage.arriving_memory(), (0, 600));

    for piece in 0..600 {
        block_of(&storage, piece, 0, piece as u8);
    }
    assert_eq!(storage.arriving_memory(), (600 * block_memory(), 600));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 600 * PIECE);

    // The rest of one piece arrives: its memory grows a block at a time.
    for block in 1..16 {
        block_of(&storage, 0, block, 7);
        assert_eq!(storage.arriving_memory().0, (600 + block) * block_memory());
    }
    let mut back = vec![0; BLOCK as usize];
    storage.pread_exact(0, 15 * BLOCK, &mut back).unwrap();
    assert_eq!(back, noise(BLOCK as usize, 7));

    drop(storage);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}

// A piece that fails its hash lets go of the memory its blocks took with its
// room. A piece whose peer left keeps what arrived and no more, and the next
// peer granted it, or the next run, writes into the same memory.
#[test]
fn a_failed_piece_lets_go_of_what_arrived_and_a_left_one_keeps_it() {
    let (layout, indexes) = pieces(4);
    let budget = accountant(u64::MAX);
    let storage = PieceStorage::new(
        layout,
        Arc::new(Recorder::default()) as Arc<dyn Pieces>,
        holds(&budget),
    );
    let room = Arc::new(Notify::new());
    for &piece in &indexes {
        assert!(storage.reserve_piece(piece, &room));
    }
    for piece in 0..4 {
        for block in 0..4 {
            block_of(&storage, piece, block, piece as u8);
        }
    }
    let four = 4 * block_memory();
    assert_eq!(storage.arriving_memory(), (4 * four, 4));

    storage.on_piece_hash_failed(indexes[0]).unwrap();
    assert_eq!(storage.arriving_memory(), (3 * four, 3));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 3 * PIECE);

    // The peer of piece 1 left: granted again, it holds what arrived.
    assert!(storage.reserve_piece(indexes[1], &room));
    assert_eq!(storage.arriving_memory(), (3 * four, 3));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 3 * PIECE);
    let mut back = vec![0; BLOCK as usize];
    storage
        .pread_exact(0, PIECE + 3 * BLOCK, &mut back)
        .unwrap();
    assert_eq!(back, noise(BLOCK as usize, 1));

    // A run that ended keeps them too, and the next run takes them up.
    drop(storage.take().unwrap());
    assert!(storage.reserve_piece(indexes[2], &room));
    assert_eq!(storage.arriving_memory(), (3 * four, 3));
}

// A piece verified and written gives its memory, its resident room and its
// volume's room back once its writer has written it; the pieces still
// arriving keep only what arrived of them.
#[test]
fn a_written_piece_gives_back_its_memory_and_its_room() {
    let (layout, indexes) = pieces(4);
    let dir = tempfile::tempdir().unwrap();
    let store = store_in(&layout, &dir.path().join("job"), None);
    let volume = store.writes().volume();
    let budget = accountant(u64::MAX);
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&store) as Arc<dyn Pieces>,
        holds(&budget),
    );
    let room = Arc::new(Notify::new());
    for &piece in &indexes {
        assert!(storage.reserve_piece(piece, &room));
    }
    assert_eq!(volume.unwritten_bytes(), 4 * PIECE);
    for block in 0..16 {
        block_of(&storage, 0, block, block as u8);
    }
    block_of(&storage, 1, 0, 1);
    assert_eq!(storage.arriving_memory(), (17 * block_memory(), 4));

    storage.on_piece_completed(indexes[0]).unwrap();
    assert_eq!(storage.arriving_memory(), (block_memory(), 3));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 3 * PIECE);
    assert_eq!(volume.unwritten_bytes(), 3 * PIECE);
    let written = std::fs::read(dir.path().join("job").join("a")).unwrap();
    for block in 0..16 {
        let at = (block * BLOCK) as usize;
        assert_eq!(
            written[at..at + BLOCK as usize],
            noise(BLOCK as usize, block as u8)
        );
    }
}
