// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use weaver_graph::NodeId;
use weaver_graph::WindowMemory;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::names::INTERNAL_DIR;

use super::{
    Refused, SPARE_DIR, ScratchLedger, Step, Tried, Why, Windows, spare_name, sweep_spare,
    window_name,
};
use crate::operations::{Capacity, CapacityReader, CapacityReading};

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

fn windows(dir: &Path) -> Arc<Windows> {
    Arc::new(Windows::new(
        dir.to_path_buf(),
        CapacityReader::unknown(),
        ScratchLedger::default(),
        accountant(0),
        super::WINDOW_CEILING_BYTES,
    ))
}

// The files left in a folder, by name.
fn files(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

// The files left in the job's chunk directory.
fn left(dir: &Path) -> Vec<String> {
    files(&dir.join(INTERNAL_DIR))
}

// The folders tried, and the step each refused at.
fn tried(error: &io::Error) -> &[Tried] {
    match &Refused::of(error).unwrap_or_else(|| panic!("{error}")).why {
        Why::Nowhere(tried) => tried,
        why => panic!("{why}"),
    }
}

fn fill(window: &mut super::Window, seed: u8) {
    for (at, byte) in window.as_mut_slice().iter_mut().enumerate() {
        *byte = (at as u8).wrapping_mul(31).wrapping_add(seed);
    }
}

fn filled(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|at| (at as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

// A window is a new file under the chunk directory, its bytes the file's,
// and dropping it removes the file and returns everything it counted.
#[test]
fn a_window_is_a_scratch_file_that_goes_when_it_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let windows = windows(dir.path());
    let len = MIB as usize;
    let mut window = windows.allocate(NodeId(3), len).unwrap();
    assert_eq!(window.path(), dir.path().join(window_name(NodeId(3), 0)));
    assert_eq!(left(dir.path()), ["graph-window-3-0.scratch"]);
    assert_eq!(WindowMemory::len(&window), len);
    let meta = std::fs::metadata(window.path()).unwrap();
    assert_eq!(meta.len(), len as u64);
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
    }
    assert_eq!(windows.live(), (1, MIB));

    fill(&mut window, 7);
    assert_eq!(window.as_mut_slice(), filled(len, 7));
    assert_eq!(std::fs::read(window.path()).unwrap(), filled(len, 7));

    drop(window);
    assert!(left(dir.path()).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// Two windows lent at once are two files: what one holds is not the
// other's, and dropping one leaves the other as it was.
#[test]
fn two_windows_at_once_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let windows = windows(dir.path());
    let mut first = windows.allocate(NodeId(1), MIB as usize).unwrap();
    let mut second = windows.allocate(NodeId(1), 2 * MIB as usize).unwrap();
    assert_ne!(first.path(), second.path());
    assert_ne!(
        first.as_mut_slice().as_ptr(),
        second.as_mut_slice().as_ptr()
    );
    assert_eq!(windows.live(), (2, 3 * MIB));

    fill(&mut first, 1);
    fill(&mut second, 2);
    let second_path = second.path().to_path_buf();
    drop(first);
    assert_eq!(left(dir.path()), ["graph-window-1-1.scratch"]);
    assert_eq!(windows.live(), (1, 2 * MIB));
    assert_eq!(second.as_mut_slice(), filled(2 * MIB as usize, 2));
    assert_eq!(
        std::fs::read(&second_path).unwrap(),
        filled(2 * MIB as usize, 2)
    );
    drop(second);
    assert!(left(dir.path()).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// A run borrows through the lender its budget carries: the window it is
// lent is as long as it asked, and goes when the decoder drops it.
#[test]
fn a_run_borrows_its_windows_through_the_lender() {
    let dir = tempfile::tempdir().unwrap();
    let windows = windows(dir.path());
    let (lender, _) = windows.for_node(NodeId(9));
    assert_eq!(lender.ceiling(), super::WINDOW_CEILING_BYTES);
    let mut lent = lender.lend(MIB as usize).unwrap();
    assert_eq!(lent.len(), MIB as usize);
    assert!(!lent.as_mut_ptr().is_null());
    assert_eq!(left(dir.path()), ["graph-window-9-0.scratch"]);
    assert_eq!(windows.live(), (1, MIB));
    drop(lent);
    assert!(left(dir.path()).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// A lender keeps the widest window a node's run asked for under its start,
// lent or refused for want of room, once one was refused so, and nothing
// else: a window over the ceiling is no window the run can hold, another
// node's is its own, and a run handed a lender again starts with none on
// record.
#[test]
fn a_lender_keeps_the_widest_window_asked_once_it_had_no_room() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Windows::new(
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
    assert_eq!(windows.live(), (1, 2 * MIB));

    drop((held, lender));
    assert_eq!(
        windows.refused(NodeId(4), start),
        Some(3 * MIB),
        "kept past the run"
    );
    let (_again, restarted) = windows.for_node(NodeId(4));
    assert_eq!(windows.refused(NodeId(4), restarted), None);
    assert_eq!(windows.refused(NodeId(4), start), None);
    assert_eq!(windows.live(), (0, 0));
}

// A lender handed for an earlier start of a node, still held by a run that
// was stopped, records nothing once the node was handed a lender again:
// the new start's record is the new run's alone.
#[test]
fn a_lender_from_an_earlier_start_records_nothing_for_the_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Windows::new(
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

// The lender says the room every folder has now past its reserve, all
// together, less what is lent; nothing where a reading is unknown or no
// reserve is kept.
#[test]
fn a_lender_says_the_room_its_folders_have() {
    let job = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let windows = Arc::new(
        Windows::new(
            job.path().to_path_buf(),
            free(10 * MIB),
            ScratchLedger::default(),
            accountant(7 * MIB),
            super::WINDOW_CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            free(9 * MIB),
            ScratchLedger::default(),
        ),
    );
    let (lender, _) = windows.for_node(NodeId(1));
    assert_eq!(lender.room(), Some(5 * MIB));
    let held = lender.lend(2 * MIB as usize).unwrap();
    assert_eq!(lender.room(), Some(3 * MIB));
    drop(held);
    assert_eq!(lender.room(), Some(5 * MIB));

    let unknown = Arc::new(
        Windows::new(
            job.path().to_path_buf(),
            free(10 * MIB),
            ScratchLedger::default(),
            accountant(7 * MIB),
            super::WINDOW_CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            CapacityReader::unknown(),
            ScratchLedger::default(),
        ),
    );
    assert_eq!(unknown.room(), None);
    assert_eq!(windows_of(job.path(), 0).room(), None, "no reserve kept");
}

fn windows_of(dir: &Path, reserve: u64) -> Arc<Windows> {
    Arc::new(Windows::new(
        dir.to_path_buf(),
        free(10 * MIB),
        ScratchLedger::default(),
        accountant(reserve),
        super::WINDOW_CEILING_BYTES,
    ))
}

// Two jobs that lend on one disk charge one ledger: room for one window is
// lent to one of them, the other is refused, and once the first is given
// back the other is lent. Each job's own count would have lent both, past
// the reserve downloads and holds keep.
#[test]
fn two_jobs_on_one_disk_never_both_take_the_same_room() {
    let (first_job, second_job) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let disk = free(10 * MIB);
    let ledger = ScratchLedger::default();
    let job = |dir: &Path, ledger: &ScratchLedger| {
        Arc::new(Windows::new(
            dir.to_path_buf(),
            disk.clone(),
            ledger.clone(),
            accountant(7 * MIB),
            super::WINDOW_CEILING_BYTES,
        ))
    };
    let (first, second) = (
        job(first_job.path(), &ledger),
        job(second_job.path(), &ledger),
    );
    let lent = first.allocate(NodeId(1), 2 * MIB as usize).unwrap();
    let error = second.allocate(NodeId(1), 2 * MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    assert_eq!(second.room(), Some(MIB));
    drop(lent);
    let lent = second.allocate(NodeId(1), 2 * MIB as usize).unwrap();
    assert_eq!(first.room(), Some(MIB));
    drop(lent);

    let apart = job(second_job.path(), &ScratchLedger::default());
    let held = first.allocate(NodeId(1), 2 * MIB as usize).unwrap();
    let beside = apart.allocate(NodeId(1), 2 * MIB as usize).unwrap();
    drop((held, beside));
}

// The file is created new: a file already at the name, or a link there,
// refuses the window, and what was there stays as it was.
#[test]
fn a_window_never_takes_an_existing_file_or_follows_a_link() {
    let dir = tempfile::tempdir().unwrap();
    let windows = windows(dir.path());
    let chunks = dir.path().join(INTERNAL_DIR);
    std::fs::create_dir_all(&chunks).unwrap();

    let taken = dir.path().join(window_name(NodeId(1), 0));
    std::fs::write(&taken, b"someone else's").unwrap();
    let error = windows.allocate(NodeId(1), MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::Create, "{error}");
    assert_eq!(std::fs::read(&taken).unwrap(), b"someone else's");

    let outside = dir.path().join("outside.bin");
    std::fs::write(&outside, b"not the job's").unwrap();
    let linked = dir.path().join(window_name(NodeId(1), 1));
    std::os::unix::fs::symlink(&outside, &linked).unwrap();
    let error = windows.allocate(NodeId(1), MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::Create, "{error}");
    assert_eq!(std::fs::read(&outside).unwrap(), b"not the job's");
    assert!(
        std::fs::symlink_metadata(&linked)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        left(dir.path()),
        ["graph-window-1-0.scratch", "graph-window-1-1.scratch"]
    );
    assert_eq!(windows.live(), (0, 0));
}

// A chunk directory that is a link leads out of the job's folder: no
// window is made through it.
#[test]
fn a_window_is_never_made_through_a_linked_chunk_directory() {
    let dir = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(elsewhere.path(), dir.path().join(INTERNAL_DIR)).unwrap();
    let windows = windows(dir.path());
    let error = windows.allocate(NodeId(1), MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::Directory, "{error}");
    assert!(files(elsewhere.path()).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// The disk keeps the holds' reserve free: a window that would take it
// below is refused before any file is made, and each window lent counts
// against the same reading until it is dropped.
#[test]
fn a_window_never_takes_the_disk_below_its_reserve() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Windows::new(
        dir.path().to_path_buf(),
        free(10 * MIB),
        ScratchLedger::default(),
        accountant(8 * MIB),
        super::WINDOW_CEILING_BYTES,
    ));
    let error = windows.allocate(NodeId(1), 3 * MIB as usize).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    assert!(left(dir.path()).is_empty());

    let first = windows.allocate(NodeId(1), MIB as usize).unwrap();
    let second = windows.allocate(NodeId(1), MIB as usize).unwrap();
    let error = windows.allocate(NodeId(1), MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    assert_eq!(left(dir.path()).len(), 2);

    drop(first);
    let third = windows.allocate(NodeId(1), MIB as usize).unwrap();
    assert_eq!(windows.live(), (2, 2 * MIB));
    drop((second, third));
    assert!(left(dir.path()).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// No window is larger than the ceiling, and none is empty.
#[test]
fn a_window_over_the_ceiling_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Windows::new(
        dir.path().to_path_buf(),
        CapacityReader::unknown(),
        ScratchLedger::default(),
        accountant(0),
        MIB,
    ));
    let error = windows.allocate(NodeId(1), MIB as usize + 1).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
    assert_eq!(
        Refused::of(&error).unwrap().why,
        Why::OverCeiling(MIB),
        "{error}"
    );
    assert!(windows.allocate(NodeId(1), 0).is_err());
    assert!(left(dir.path()).is_empty());
    let window = windows.allocate(NodeId(1), MIB as usize).unwrap();
    drop(window);
    assert_eq!(windows.live(), (0, 0));
}

// A step that fails after the file was made, here a preallocation no disk
// can give, removes the file and gives back what it charged.
#[test]
fn a_failed_step_leaves_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Windows::new(
        dir.path().to_path_buf(),
        free(u64::MAX / 2),
        ScratchLedger::default(),
        accountant(MIB),
        u64::MAX,
    ));
    let error = windows.allocate(NodeId(1), 1 << 60).unwrap_err();
    let step = tried(&error)[0].step;
    assert!(
        matches!(
            step,
            Step::NoSpace | Step::Preallocate | Step::PreallocateUnsupported
        ),
        "{error}"
    );
    assert!(left(dir.path()).is_empty());
    assert_eq!(windows.live(), (0, 0));
    let window = windows.allocate(NodeId(1), MIB as usize).unwrap();
    drop(window);
    assert!(left(dir.path()).is_empty());
}

// Where the job's folder refuses a window, it goes under the data folder's
// window subfolder, named for the job, and goes from there when dropped.
#[test]
fn the_data_folder_takes_a_window_the_jobs_folder_refuses() {
    let job = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let windows = Arc::new(
        Windows::new(
            job.path().to_path_buf(),
            free(10 * MIB),
            ScratchLedger::default(),
            accountant(8 * MIB),
            super::WINDOW_CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            free(100 * MIB),
            ScratchLedger::default(),
        ),
    );
    let mut window = windows.allocate(NodeId(5), 4 * MIB as usize).unwrap();
    assert_eq!(
        window.path(),
        data.path().join(spare_name(42, NodeId(5), 0))
    );
    assert!(left(job.path()).is_empty());
    assert_eq!(
        files(&data.path().join(SPARE_DIR)),
        ["graph-window-42-5-0.scratch"]
    );
    fill(&mut window, 3);
    assert_eq!(
        std::fs::read(window.path()).unwrap(),
        filled(4 * MIB as usize, 3)
    );
    assert_eq!(windows.live(), (1, 4 * MIB));

    // A window the job's folder can hold still goes there first.
    let small = windows.allocate(NodeId(5), MIB as usize).unwrap();
    assert_eq!(left(job.path()), ["graph-window-5-1.scratch"]);

    drop((window, small));
    assert!(left(job.path()).is_empty());
    assert!(files(&data.path().join(SPARE_DIR)).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// Where both folders refuse, the refusal names the bytes and, for each
// folder in the order tried, the step it refused at; neither keeps a file.
#[test]
fn a_window_neither_folder_holds_is_refused_with_both_reasons() {
    let job = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(elsewhere.path(), job.path().join(INTERNAL_DIR)).unwrap();
    let windows = Arc::new(
        Windows::new(
            job.path().to_path_buf(),
            CapacityReader::unknown(),
            ScratchLedger::default(),
            accountant(8 * MIB),
            super::WINDOW_CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            free(10 * MIB),
            ScratchLedger::default(),
        ),
    );
    let error = windows.allocate(NodeId(5), 4 * MIB as usize).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
    let refused = Refused::of(&error).unwrap();
    assert_eq!(refused.bytes, 4 * MIB);
    let tried = tried(&error);
    assert_eq!(tried.len(), 2, "{error}");
    assert_eq!(tried[0].folder, job.path().join(INTERNAL_DIR));
    assert_eq!(tried[0].step, Step::Directory);
    assert_eq!(tried[1].folder, data.path().join(SPARE_DIR));
    assert_eq!(tried[1].step, Step::NoSpace);
    let message = error.to_string();
    assert!(message.contains(&(4 * MIB).to_string()), "{message}");
    assert!(message.contains("not enough free space"), "{message}");
    assert!(files(elsewhere.path()).is_empty());
    assert!(files(&data.path().join(SPARE_DIR)).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// Startup removes the window files a past process left in the data
// folder's window subfolder, and nothing else there or beside it.
#[test]
fn startup_sweeps_only_window_files_in_the_data_folder() {
    let data = tempfile::tempdir().unwrap();
    let spare = data.path().join(SPARE_DIR);
    std::fs::create_dir_all(spare.join("inner")).unwrap();
    for name in [spare_name(1, NodeId(0), 0), spare_name(7, NodeId(3), 9)] {
        std::fs::write(data.path().join(name), b"window").unwrap();
    }
    std::fs::write(spare.join("notes.txt"), b"keep").unwrap();
    std::fs::write(spare.join("inner/graph-window-1-1-1.scratch"), b"keep").unwrap();
    std::fs::write(data.path().join("graph-window-1-0-0.scratch"), b"keep").unwrap();
    std::fs::write(data.path().join("weaver.db"), b"keep").unwrap();

    sweep_spare(data.path());

    assert_eq!(files(&spare), ["inner", "notes.txt"]);
    assert_eq!(files(&spare.join("inner")), ["graph-window-1-1-1.scratch"]);
    assert_eq!(
        files(data.path()),
        [SPARE_DIR, "graph-window-1-0-0.scratch", "weaver.db"]
    );
}

// A window subfolder that is a link is not Weaver's: the sweep does not
// follow it.
#[test]
fn startup_never_sweeps_through_a_linked_window_folder() {
    let data = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    std::fs::write(elsewhere.path().join("graph-window-1-0-0.scratch"), b"keep").unwrap();
    std::os::unix::fs::symlink(elsewhere.path(), data.path().join(SPARE_DIR)).unwrap();
    sweep_spare(data.path());
    assert_eq!(files(elsewhere.path()), ["graph-window-1-0-0.scratch"]);
}
