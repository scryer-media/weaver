// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use weaver_graph::NodeId;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::names::INTERNAL_DIR;

use super::Windows;
use crate::operations::{Capacity, CapacityReader, CapacityReading};
use weaver_server_disk::scratch::{CEILING_BYTES, Scratch, ScratchLedger};

const MIB: u64 = 1 << 20;

fn accountant(disk_reserve: u64) -> Arc<HoldsAccountant> {
    Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve,
    }))
}

fn free(available_bytes: u64) -> CapacityReader {
    let sampled_at = Instant::now();
    CapacityReader::from_fn(move || {
        Capacity::Known(CapacityReading {
            available_bytes,
            total_bytes: available_bytes,
            sampled_at,
            stale: false,
        })
    })
}

fn over(scratch: Scratch) -> Arc<Windows> {
    Arc::new(Windows::new(Arc::new(scratch)))
}

fn windows(dir: &Path) -> Arc<Windows> {
    over(Scratch::new(
        dir.to_path_buf(),
        CapacityReader::unknown(),
        ScratchLedger::default(),
        accountant(0),
        CEILING_BYTES,
    ))
}

// The files left in the job's chunk directory, by name.
fn left(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir.join(INTERNAL_DIR)) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

// A run borrows through the lender its budget carries: the window it is
// lent is as long as it asked, and goes when the decoder drops it.
#[test]
fn a_run_borrows_its_windows_through_the_lender() {
    let dir = tempfile::tempdir().unwrap();
    let windows = windows(dir.path());
    let (lender, _) = windows.for_node(NodeId(9));
    assert_eq!(lender.ceiling(), CEILING_BYTES);
    let mut lent = lender.lend(MIB as usize).unwrap();
    assert_eq!(lent.len(), MIB as usize);
    assert!(!lent.as_mut_ptr().is_null());
    assert_eq!(left(dir.path()), ["graph-window-9-0.scratch"]);
    assert_eq!(windows.scratch().live(), (1, MIB));
    drop(lent);
    assert!(left(dir.path()).is_empty());
    assert_eq!(windows.scratch().live(), (0, 0));
}

// A lender keeps the widest window a node's run asked for under its start,
// lent or refused for want of room, once one was refused so, and nothing
// else: a window over the ceiling is no window the run can hold, another
// node's is its own, and a run handed a lender again starts with none on
// record.
#[test]
fn a_lender_keeps_the_widest_window_asked_once_it_had_no_room() {
    let dir = tempfile::tempdir().unwrap();
    let windows = over(Scratch::new(
        dir.path().to_path_buf(),
        free(10 * MIB),
        ScratchLedger::default(),
        accountant(7 * MIB),
        4 * MIB,
    ));
    let (lender, start) = windows.for_node(NodeId(4));
    assert_eq!(windows.refused(NodeId(4), start), None);
    let held = lender.lend(2 * MIB as usize).unwrap();
    assert_eq!(windows.refused(NodeId(4), start), None, "lent");

    let Err(error) = lender.lend(5 * MIB as usize) else {
        panic!("lent over the ceiling");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
    assert_eq!(windows.refused(NodeId(4), start), None, "over the ceiling");

    for (asked, widest) in [(2 * MIB, 2 * MIB), (3 * MIB, 3 * MIB), (2 * MIB, 3 * MIB)] {
        let Err(error) = lender.lend(asked as usize) else {
            panic!("lent {asked} bytes");
        };
        assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
        assert_eq!(windows.refused(NodeId(4), start), Some(widest));
    }
    let (_, other) = windows.for_node(NodeId(5));
    assert_eq!(windows.refused(NodeId(5), other), None);
    assert_eq!(windows.scratch().live(), (1, 2 * MIB));

    drop((held, lender));
    assert_eq!(
        windows.refused(NodeId(4), start),
        Some(3 * MIB),
        "kept past the run"
    );
    let (_again, restarted) = windows.for_node(NodeId(4));
    assert_eq!(windows.refused(NodeId(4), restarted), None);
    assert_eq!(windows.refused(NodeId(4), start), None);
    assert_eq!(windows.scratch().live(), (0, 0));
}

// A lender handed for an earlier start of a node, still held by a run that
// was stopped, records nothing once the node was handed a lender again:
// the new start's record is the new run's alone.
#[test]
fn a_lender_from_an_earlier_start_records_nothing_for_the_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let windows = over(Scratch::new(
        dir.path().to_path_buf(),
        free(2 * MIB),
        ScratchLedger::default(),
        accountant(MIB),
        4 * MIB,
    ));
    let (old, first) = windows.for_node(NodeId(4));
    let (new, second) = windows.for_node(NodeId(4));
    assert_ne!(first, second);
    let Err(error) = old.lend(2 * MIB as usize) else {
        panic!("lent past the reserve");
    };
    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    assert_eq!(windows.refused(NodeId(4), second), None);
    assert_eq!(windows.refused(NodeId(4), first), None, "no longer kept");
    let Err(_) = new.lend(2 * MIB as usize) else {
        panic!("lent past the reserve");
    };
    assert_eq!(windows.refused(NodeId(4), second), Some(2 * MIB));
}

// The lender says the most room any one folder has now past its reserve,
// less what is lent; nothing where a reading is unknown or no reserve is
// kept. The job's folder has 3 MiB, the data folder 2 MiB: 3 MiB, not 5.
// With 2 MiB lent in the job's folder, it has 1 MiB, and the data folder,
// which may be the same disk, is counted less the 2 MiB lent beside it.
#[test]
fn a_lender_says_the_room_its_folders_have() {
    let job = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let windows = over(
        Scratch::new(
            job.path().to_path_buf(),
            free(10 * MIB),
            ScratchLedger::default(),
            accountant(7 * MIB),
            CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            free(9 * MIB),
            ScratchLedger::default(),
        ),
    );
    let (lender, _) = windows.for_node(NodeId(1));
    assert_eq!(lender.room(), Some(3 * MIB));
    let held = lender.lend(2 * MIB as usize).unwrap();
    assert_eq!(lender.room(), Some(MIB));
    drop(held);
    assert_eq!(lender.room(), Some(3 * MIB));

    let unknown = over(
        Scratch::new(
            job.path().to_path_buf(),
            free(10 * MIB),
            ScratchLedger::default(),
            accountant(7 * MIB),
            CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            CapacityReader::unknown(),
            ScratchLedger::default(),
        ),
    );
    assert_eq!(unknown.scratch().room(), None);
    let unkept = over(Scratch::new(
        job.path().to_path_buf(),
        free(10 * MIB),
        ScratchLedger::default(),
        accountant(0),
        CEILING_BYTES,
    ));
    assert_eq!(unkept.scratch().room(), None, "no reserve kept");
}

// The job's folder inside the data folder, one disk read through one
// reader, each folder on a ledger of its own as the pipeline keeps them:
// 10 MiB free, 7 MiB kept, is room for 3 MiB, not 6, and once 3 MiB is
// lent there is none, in either folder.
#[test]
fn two_folders_on_one_disk_say_its_room_once() {
    let data = tempfile::tempdir().unwrap();
    let job = data.path().join("job");
    std::fs::create_dir(&job).unwrap();
    let disk = free(10 * MIB);
    let windows = over(
        Scratch::new(
            job,
            disk.clone(),
            ScratchLedger::default(),
            accountant(7 * MIB),
            CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            disk,
            ScratchLedger::default(),
        ),
    );
    let (lender, _) = windows.for_node(NodeId(1));
    assert_eq!(lender.room(), Some(3 * MIB));
    let held = lender.lend(3 * MIB as usize).unwrap();
    assert_eq!(lender.room(), Some(0));
    drop(held);
    assert_eq!(lender.room(), Some(3 * MIB));
}
