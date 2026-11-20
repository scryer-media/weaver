// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::graph::job::GraphError;
use crate::pipeline::graph::limits::Device;
use crate::pipeline::graph::namespace::WriteError;

// A loan with nothing taken, working in `dir`.
fn bare(dir: &std::path::Path) -> Lent {
    Lent {
        namespace: NodeId(1),
        set: NodeId(2),
        recovery: Some(NodeId(2)),
        defer_holds: false,
        scratch_read: None,
        held_scratch: BTreeMap::new(),
        settled: None,
        scratch_later: BTreeMap::new(),
        unwritten: false,
        internal: dir.to_path_buf(),
        members: BTreeMap::new(),
        homes: BTreeMap::new(),
        usage: RefCell::default(),
        postings: BTreeMap::new(),
        shelves: BTreeMap::new(),
        volumes: BTreeMap::new(),
        window: None,
        open: RefCell::new(None),
        reads: 0,
        repaired: Repaired::again(dir, NodeId(2)).expect("an output"),
        found: Vec::new(),
        diagnostics: Vec::new(),
        scratch: None,
        shutdown: Shutdown::default(),
        withheld: BTreeSet::new(),
        working: None,
        reading: RefCell::default(),
        stop: Arc::default(),
        timing: RefCell::default(),
    }
}

// A repair or replay waiting for a read turn that the volume's other
// readers hold ends when its job stops the loan: the read is not made,
// and the loan reads as stopped, so nothing it came to stands. On solid
// state and on a spinning volume alike.
#[test]
fn a_loan_waiting_for_a_turn_ends_when_its_job_stops_it() {
    use weaver_server_disk::operations::disk::StorageKind;
    for kind in [StorageKind::Solid, StorageKind::Rotational] {
        let dir = tempfile::tempdir().expect("a working directory");
        let volume = Arc::new(Volume::new(None, "test".to_owned()));
        let reads = volume.reads(|| kind);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let every = runtime
            .block_on(Arc::clone(reads.turns()).acquire_many_owned(reads.readers() as u32))
            .unwrap();
        let lent = bare(dir.path());
        assert!(!lent.stopped());
        let stop = lent.stop();
        let waiting = {
            let volume = Arc::clone(&volume);
            std::thread::spawn(move || {
                let read = lent.in_turn(Some(&volume), || "stopped", || "read");
                (read, lent.stopped())
            })
        };
        stop.await_waiters(1);
        stop.raise();
        assert_eq!(waiting.join().unwrap(), ("stopped", true), "{kind:?}");
        drop(every);
        assert_eq!(
            reads.turns().available_permits(),
            reads.readers(),
            "{kind:?}"
        );
    }
}

// A repair whose output finds the disk full part-way comes back as a
// write the working disk had no room for, which holds the job, whatever
// kind of recovery wrote it: every kind writes what it rebuilt here.
#[test]
fn a_repair_whose_output_finds_the_disk_full_comes_back_out_of_space() {
    use crate::pipeline::graph::leaf::counting::fill_after;
    let dir = tempfile::tempdir().expect("a working directory");
    let mut repaired = Repaired::again(dir.path(), NodeId(4)).expect("an output");
    // A repair that writes 500 MB finds the disk full 14 bytes in.
    repaired.will_write(500 << 20);
    fill_after(Some(10));
    repaired.write(MemberId(0), 0, b"rebuilt");
    repaired.write(MemberId(0), 7, b"rebuilt");
    fill_after(None);
    assert!(!repaired.output.path().exists());
    // What it needs free to run again: all it writes, not the 14 bytes
    // it had written when it failed.
    assert_eq!(repaired.gave_back(), 500 << 20);

    // One that did not say what it writes names no figure.
    let mut unsaid = Repaired::again(dir.path(), NodeId(5)).expect("an output");
    fill_after(Some(0));
    unsaid.write(MemberId(0), 0, b"rebuilt");
    fill_after(None);
    assert_eq!(unsaid.gave_back(), 0);

    let error = repaired.failed().expect("the repair failed");
    let error = GraphError::from(WriteError::from(error));
    assert_eq!(error.out_of_space(), Some(Device::Working));
}

// A repair whose scratch directory found the disk full comes back as a
// write the working disk had no room for, which holds the job: nothing
// it staged is applied, and its output is gone.
#[test]
fn a_repair_whose_scratch_found_the_disk_full_comes_back_out_of_space() {
    let dir = tempfile::tempdir().expect("a working directory");
    let mut repaired = Repaired::again(dir.path(), NodeId(3)).expect("an output");
    // A restore that stages 5 GB and rebuilds 200 MB, which it then
    // copies into its member.
    let whole = (5u64 << 30) + 2 * (200 << 20);
    repaired.will_write(whole);
    repaired.write(MemberId(0), 0, b"rebuilt");
    let output = repaired.output.path().to_path_buf();
    assert!(output.exists());

    let scratch = dir.path().join("scratch");
    repaired.refuse(
        &scratch,
        std::io::Error::from(std::io::ErrorKind::StorageFull),
    );
    // A later write is not made, and a second refusal does not replace
    // the first.
    repaired.write(MemberId(0), 7, b"more");
    repaired.refuse(&scratch, std::io::Error::from(std::io::ErrorKind::NotFound));
    assert!(!output.exists());
    // What it needs free to run again: all it stages, rebuilds and
    // copies, not what it had staged when it failed.
    assert_eq!(repaired.gave_back(), whole);

    let error = repaired.failed().expect("the repair failed");
    let error = GraphError::from(WriteError::from(error));
    assert_eq!(error.out_of_space(), Some(Device::Working));
}
