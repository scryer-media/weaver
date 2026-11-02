// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::readers::{CAPACITY, IDLE, ReaderPool};
use super::*;

// A pool of this test's own, which no other test reads through.
fn pool() -> &'static ReaderPool {
    Box::leak(Box::new(ReaderPool::new()))
}

// A covered leaf of `bytes` read through `readers`, its write handle let go.
fn closed(readers: &'static ReaderPool, dir: &Path, name: &str, bytes: &[u8]) -> Leaf {
    let mut leaf = Leaf::at(dir, name).unwrap().with_readers(readers);
    leaf.write(0, bytes, Some(bytes.len() as u64)).unwrap();
    leaf.close(bytes.len() as u64).unwrap();
    leaf
}

fn read(leaf: &Leaf, offset: u64, len: usize) -> Vec<u8> {
    let mut out = vec![0; len];
    leaf.read_shared_exact(offset, &mut out).unwrap();
    out
}

#[test]
fn a_closed_leaf_is_read_through_one_handle_that_follows_none_of_its_moves() {
    let dir = tempfile::tempdir().unwrap();
    let mut leaf = closed(pool(), dir.path(), "a.bin", b"0123456789");
    assert!(!leaf.reader_is_open());

    assert_eq!(read(&leaf, 2, 3), b"234");
    assert!(leaf.reader_is_open());
    let mut appended = b"x".to_vec();
    leaf.read(5, 2, &mut appended).unwrap();
    assert_eq!(appended, b"x56");
    // A plain read opens the file for itself and keeps nothing.
    let mut plain = vec![0; 2];
    let other = closed(pool(), dir.path(), "c.bin", b"cd");
    other.read_exact(0, &mut plain).unwrap();
    assert_eq!(plain, b"cd");
    assert!(!other.reader_is_open());

    // Renamed, the handle is let go and the file read where it now is.
    leaf.rename(dir.path(), "b.bin").unwrap();
    assert!(!leaf.reader_is_open());
    assert_eq!(read(&leaf, 0, 2), b"01");
    assert!(leaf.reader_is_open());

    // Let go of ahead of a move, as the job's directory moves.
    leaf.release();
    assert!(!leaf.reader_is_open());
    assert_eq!(read(&leaf, 8, 2), b"89");

    // Written again, reads go through the write handle.
    leaf.write(0, b"Z", None).unwrap();
    assert!(!leaf.reader_is_open());
    assert_eq!(read(&leaf, 0, 2), b"Z1");
}

#[test]
fn a_leaf_removed_or_dropped_holds_no_handle() {
    let dir = tempfile::tempdir().unwrap();
    let readers = pool();
    let leaf = closed(readers, dir.path(), "a.bin", b"abc");
    assert_eq!(read(&leaf, 0, 3), b"abc");
    let key = leaf.reader;
    leaf.remove().unwrap();
    assert!(!readers.is_open(key));

    let leaf = closed(readers, dir.path(), "b.bin", b"abc");
    assert_eq!(read(&leaf, 0, 3), b"abc");
    let key = leaf.reader;
    drop(leaf);
    assert!(!readers.is_open(key));
}

#[test]
fn past_the_capacity_the_least_recently_read_handle_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let readers = pool();
    let leaves: Vec<Leaf> = (0..=CAPACITY)
        .map(|n| closed(readers, dir.path(), &format!("{n}.bin"), b"ab"))
        .collect();
    for leaf in &leaves[..CAPACITY] {
        assert_eq!(read(leaf, 0, 1), b"a");
    }
    // The first is read again, so the second is the least recently read.
    assert_eq!(read(&leaves[0], 1, 1), b"b");
    assert_eq!(read(&leaves[CAPACITY], 0, 2), b"ab");
    assert!(leaves[0].reader_is_open());
    assert!(!leaves[1].reader_is_open());
    assert!(leaves[CAPACITY].reader_is_open());
}

#[tokio::test(start_paused = true)]
async fn a_handle_unread_for_the_idle_time_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let readers = pool();
    let kept = closed(readers, dir.path(), "kept.bin", b"ab");
    let idle = closed(readers, dir.path(), "idle.bin", b"ab");
    assert_eq!(read(&idle, 0, 2), b"ab");
    assert_eq!(read(&kept, 0, 2), b"ab");

    // Read again short of the idle time, `kept` stays open past it.
    tokio::time::advance(IDLE / 2).await;
    assert_eq!(read(&kept, 0, 1), b"a");
    tokio::time::advance(IDLE / 2).await;
    while idle.reader_is_open() {
        tokio::task::yield_now().await;
    }
    assert!(kept.reader_is_open());

    tokio::time::advance(IDLE / 2).await;
    while kept.reader_is_open() {
        tokio::task::yield_now().await;
    }
}
