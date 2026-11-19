// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::names::INTERNAL_DIR;

use super::{
    Refused, SPARE_DIR, Scratch, ScratchLedger, Step, Tried, Why, file_name, spare_name,
    sweep_spare,
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

fn windows(dir: &Path) -> Arc<Scratch> {
    Arc::new(Scratch::new(
        dir.to_path_buf(),
        CapacityReader::unknown(),
        ScratchLedger::default(),
        accountant(0),
        super::CEILING_BYTES,
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

fn fill(window: &mut super::Lease, seed: u8) {
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
    let mut window = windows.allocate(3, len).unwrap();
    assert_eq!(window.path(), dir.path().join(file_name(3, 0)));
    assert_eq!(left(dir.path()), ["graph-window-3-0.scratch"]);
    assert_eq!(window.len(), len);
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
    let mut first = windows.allocate(1, MIB as usize).unwrap();
    let mut second = windows.allocate(1, 2 * MIB as usize).unwrap();
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

// A window given back after its disk was read again takes back nothing
// from the ledger: the newer reading counts its file, and the charge on the
// ledger now is another job's. On 10 MiB free with 7 MiB kept, the first
// job lends 2 MiB; the disk then reads 8 MiB free; the second job lends 1
// MiB. Giving the first back leaves the second's charge: a further 1 MiB is
// refused until the disk is read again.
#[test]
fn a_window_given_back_after_a_new_reading_takes_back_only_its_own_charge() {
    let (first_job, second_job) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let taken = std::time::Instant::now();
    let reading = Arc::new(std::sync::Mutex::new((10 * MIB, taken)));
    let disk = {
        let reading = Arc::clone(&reading);
        CapacityReader::from_fn(move || {
            let (available_bytes, sampled_at) = *reading.lock().unwrap();
            Capacity::Known(CapacityReading {
                available_bytes,
                total_bytes: 100 * MIB,
                sampled_at,
                stale: false,
            })
        })
    };
    let ledger = ScratchLedger::default();
    let job = |dir: &Path| {
        Arc::new(Scratch::new(
            dir.to_path_buf(),
            disk.clone(),
            ledger.clone(),
            accountant(7 * MIB),
            super::CEILING_BYTES,
        ))
    };
    let (first, second) = (job(first_job.path()), job(second_job.path()));
    let lent = first.allocate(1, 2 * MIB as usize).unwrap();
    *reading.lock().unwrap() = (8 * MIB, taken + std::time::Duration::from_secs(5));
    let beside = second.allocate(1, MIB as usize).unwrap();
    drop(lent);
    let error = second.allocate(2, MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    drop(beside);
    let again = second.allocate(2, MIB as usize).unwrap();
    drop(again);
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
        Arc::new(Scratch::new(
            dir.to_path_buf(),
            disk.clone(),
            ledger.clone(),
            accountant(7 * MIB),
            super::CEILING_BYTES,
        ))
    };
    let (first, second) = (
        job(first_job.path(), &ledger),
        job(second_job.path(), &ledger),
    );
    let lent = first.allocate(1, 2 * MIB as usize).unwrap();
    let error = second.allocate(1, 2 * MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    drop(lent);
    let lent = second.allocate(1, 2 * MIB as usize).unwrap();
    let error = first.allocate(1, 2 * MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    drop(lent);

    let apart = job(second_job.path(), &ScratchLedger::default());
    let held = first.allocate(1, 2 * MIB as usize).unwrap();
    let beside = apart.allocate(1, 2 * MIB as usize).unwrap();
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

    let taken = dir.path().join(file_name(1, 0));
    std::fs::write(&taken, b"someone else's").unwrap();
    let error = windows.allocate(1, MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::Create, "{error}");
    assert_eq!(std::fs::read(&taken).unwrap(), b"someone else's");

    let outside = dir.path().join("outside.bin");
    std::fs::write(&outside, b"not the job's").unwrap();
    let linked = dir.path().join(file_name(1, 1));
    std::os::unix::fs::symlink(&outside, &linked).unwrap();
    let error = windows.allocate(1, MIB as usize).unwrap_err();
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
    let error = windows.allocate(1, MIB as usize).unwrap_err();
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
    let windows = Arc::new(Scratch::new(
        dir.path().to_path_buf(),
        free(10 * MIB),
        ScratchLedger::default(),
        accountant(8 * MIB),
        super::CEILING_BYTES,
    ));
    let error = windows.allocate(1, 3 * MIB as usize).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    assert!(left(dir.path()).is_empty());

    let first = windows.allocate(1, MIB as usize).unwrap();
    let second = windows.allocate(1, MIB as usize).unwrap();
    let error = windows.allocate(1, MIB as usize).unwrap_err();
    assert_eq!(tried(&error)[0].step, Step::NoSpace, "{error}");
    assert_eq!(left(dir.path()).len(), 2);

    drop(first);
    let third = windows.allocate(1, MIB as usize).unwrap();
    assert_eq!(windows.live(), (2, 2 * MIB));
    drop((second, third));
    assert!(left(dir.path()).is_empty());
    assert_eq!(windows.live(), (0, 0));
}

// No window is larger than the ceiling, and none is empty.
#[test]
fn a_window_over_the_ceiling_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Scratch::new(
        dir.path().to_path_buf(),
        CapacityReader::unknown(),
        ScratchLedger::default(),
        accountant(0),
        MIB,
    ));
    let error = windows.allocate(1, MIB as usize + 1).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
    assert_eq!(
        Refused::of(&error).unwrap().why,
        Why::OverCeiling(MIB),
        "{error}"
    );
    assert!(windows.allocate(1, 0).is_err());
    assert!(left(dir.path()).is_empty());
    let window = windows.allocate(1, MIB as usize).unwrap();
    drop(window);
    assert_eq!(windows.live(), (0, 0));
}

// A step that fails after the file was made, here a preallocation no disk
// can give, removes the file and gives back what it charged.
#[test]
fn a_failed_step_leaves_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Scratch::new(
        dir.path().to_path_buf(),
        free(u64::MAX / 2),
        ScratchLedger::default(),
        accountant(MIB),
        u64::MAX,
    ));
    let error = windows.allocate(1, 1 << 60).unwrap_err();
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
    let window = windows.allocate(1, MIB as usize).unwrap();
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
        Scratch::new(
            job.path().to_path_buf(),
            free(10 * MIB),
            ScratchLedger::default(),
            accountant(8 * MIB),
            super::CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            free(100 * MIB),
            ScratchLedger::default(),
        ),
    );
    let mut window = windows.allocate(5, 4 * MIB as usize).unwrap();
    assert_eq!(window.path(), data.path().join(spare_name(42, 5, 0)));
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
    let small = windows.allocate(5, MIB as usize).unwrap();
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
        Scratch::new(
            job.path().to_path_buf(),
            CapacityReader::unknown(),
            ScratchLedger::default(),
            accountant(8 * MIB),
            super::CEILING_BYTES,
        )
        .or_spare(
            data.path().to_path_buf(),
            42,
            free(10 * MIB),
            ScratchLedger::default(),
        ),
    );
    let error = windows.allocate(5, 4 * MIB as usize).unwrap_err();
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
    for name in [spare_name(1, 0, 0), spare_name(7, 3, 9)] {
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
