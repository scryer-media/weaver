// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant, HoldsNamespace};
use weaver_graph::{
    ByteRange, Extraction, Lost, MemberId, NodeId, Provenance, Seek, Sink, VolumeIndex, Volumes,
    Weight, Why,
};

use super::echo::{Echoes, HEADER, SIGNATURE};
use super::volumes::RunVolumes;
use super::{DecodePool, Heard, Lent, RUN_OVERHEAD, Reservation, RunMail, Runs};
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::envelope::{Envelopes, Shelf, envelope_name};
use crate::pipeline::graph::job::{GraphJob, RootFile};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::intake::Decoded;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::status::RepairSlots;

const NODE: NodeId = NodeId(7);
const FIRST: VolumeIndex = VolumeIndex(0);

fn shelved(dir: &Path) -> (Envelopes, Arc<Shelf>) {
    let mut envelopes = Envelopes::new(dir.to_path_buf());
    let shelf = envelopes.shelf(NODE);
    (envelopes, shelf)
}

fn close(shelf: &Shelf, volume: u32, len: u64, settled: bool) {
    shelf.update(|state| {
        state.volumes.insert(volume);
        let kept = shelf.kept(state, volume).unwrap();
        kept.closed = Some(len);
        kept.settled = settled;
    });
}

fn reader(shelf: &Arc<Shelf>) -> Arc<RunVolumes> {
    Arc::new(RunVolumes::new(Arc::clone(shelf), shelf.begin()))
}

fn read(volumes: &RunVolumes, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut into = vec![0; len];
    let read = volumes.read_at(FIRST, offset, &mut into)?;
    into.truncate(read);
    Ok(into)
}

#[test]
fn a_read_waits_for_its_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"front", Provenance::Fresh)
        .unwrap();
    let volumes = reader(&shelf);
    let waiting = Arc::clone(&volumes);
    let read = std::thread::spawn(move || read(&waiting, 4, 8));
    envelopes
        .keep(NODE, 0, 5, b"-and-back", Provenance::Fresh)
        .unwrap();
    assert_eq!(read.join().unwrap().unwrap(), b"t-and-ba");
    assert_eq!(shelf.reached(), Some((FIRST, 12)));
}

// Damaged bytes are read only once repaired: the read waits over them as it
// waits for bytes still on their way, and the repair lands ahead of what the
// run read, so the run goes on reading and is never stopped.
#[test]
fn a_read_over_damaged_bytes_waits_for_their_repair() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"front", Provenance::Fresh)
        .unwrap();
    envelopes
        .keep(NODE, 0, 5, b"-xnd-back", Provenance::Damaged)
        .unwrap();
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 0, 5).unwrap(), b"front");
    let waiting = Arc::clone(&volumes);
    let read = std::thread::spawn(move || read(&waiting, 4, 8));
    // A change that leaves the range damaged does not let it through.
    close(&shelf, 0, 14, false);
    envelopes
        .keep(NODE, 0, 5, b"-and-back", Provenance::Repaired)
        .unwrap();
    assert_eq!(read.join().unwrap().unwrap(), b"t-and-ba");
    assert!(volumes.current(), "the run was never stopped");
    assert_eq!(shelf.reached(), Some((FIRST, 12)));
}

// Damaged bytes nothing will repair are lost to a read, as missing ones are:
// the read ends short where they begin, and the next fails on exactly them.
#[test]
fn a_read_over_damaged_bytes_nothing_will_repair_fails_as_lost() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"front", Provenance::Fresh)
        .unwrap();
    envelopes
        .keep(NODE, 0, 5, b"-xnd-back", Provenance::Damaged)
        .unwrap();
    close(&shelf, 0, 14, false);
    let volumes = reader(&shelf);
    let waiting = Arc::clone(&volumes);
    let first = std::thread::spawn(move || read(&waiting, 4, 8));
    close(&shelf, 0, 14, true);
    assert_eq!(first.join().unwrap().unwrap(), b"t");
    let error = read(&volumes, 5, 7).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let lost = Lost::of(&error).unwrap();
    assert_eq!(
        lost,
        Lost {
            volume: FIRST,
            range: ByteRange::new(5, 12)
        }
    );
    assert!(volumes.lost());
}

// A volume is settled for a run only once no read of it waits or fails:
// closed, and every byte of it in, fresh or repaired. A closed volume whose
// damage nothing will repair is not, though no more bytes will come for it,
// for a read over the damage fails; once repaired, it is.
#[test]
fn a_volume_is_settled_once_closed_and_every_byte_of_it_is_verified() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"front", Provenance::Fresh)
        .unwrap();
    envelopes
        .keep(NODE, 0, 5, b"-xnd-back", Provenance::Damaged)
        .unwrap();
    let volumes = reader(&shelf);
    assert!(!volumes.settled(FIRST), "open");
    close(&shelf, 0, 14, true);
    assert!(!volumes.settled(FIRST), "damaged");
    envelopes
        .keep(NODE, 0, 5, b"-and-back", Provenance::Repaired)
        .unwrap();
    assert!(volumes.settled(FIRST));
}

#[test]
fn a_read_ends_short_where_a_closed_volume_ends() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"0123456789", Provenance::Fresh)
        .unwrap();
    close(&shelf, 0, 10, false);
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 6, 8).unwrap(), b"6789");
    assert_eq!(read(&volumes, 10, 8).unwrap(), b"");
    assert_eq!(volumes.len(FIRST), Some(10));
    assert_eq!(volumes.next(None).unwrap(), Some(FIRST));
    assert_eq!(volumes.next(Some(FIRST)).unwrap(), None);
}

// An index orders the volumes: once no volume can join, a run walks the
// bound ones in order, past the gaps between them.
#[test]
fn a_complete_set_is_walked_in_index_order_past_its_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let (_envelopes, shelf) = shelved(dir.path());
    shelf.update(|state| {
        state.volumes = std::collections::BTreeSet::from([0, 1, u32::MAX]);
        state.complete = true;
    });
    let volumes = reader(&shelf);
    let mut walked = Vec::new();
    let mut at = None;
    while let Some(volume) = volumes.next(at).unwrap() {
        walked.push(volume);
        at = Some(volume);
    }
    assert_eq!(
        walked,
        [VolumeIndex(0), VolumeIndex(1), VolumeIndex(u32::MAX)]
    );
}

// A lone zip binds after every numbered volume a set could have, so `next`
// hands it out only once none can join. A run that reads the set from its
// tail is handed it at once: the volumes bound now, and its declared length,
// which puts the tail where the run reads it while the volume still grows.
#[test]
fn a_lone_volume_bound_after_every_numbered_one_is_read_before_the_set_is_complete() {
    const LAST: VolumeIndex = VolumeIndex(u32::MAX);
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, LAST.0, 96, b"tail", Provenance::Fresh)
        .unwrap();
    shelf.update(|state| {
        state.volumes.insert(LAST.0);
        state.deciding = true;
        shelf.kept(state, LAST.0).unwrap().sized = Some(100);
    });
    let volumes = reader(&shelf);
    assert_eq!(volumes.bound().unwrap(), [LAST]);
    assert_eq!(volumes.sized(LAST).unwrap(), Some(100));
    let mut into = [0; 4];
    assert_eq!(volumes.read_at(LAST, 96, &mut into).unwrap(), 4);
    assert_eq!(&into, b"tail");
    let state = shelf.lock();
    assert!(!state.complete, "the set is not complete");
    assert_eq!(
        state.kept[&LAST.0].length_read,
        Some(100),
        "the declared length is one the run built on"
    );
}

// A declared length is waited for while the volume can still bind, and
// none is given once it never will.
#[test]
fn a_declared_length_waits_for_its_volume_and_is_none_once_none_can_join() {
    let dir = tempfile::tempdir().unwrap();
    let (_envelopes, shelf) = shelved(dir.path());
    let volumes = reader(&shelf);
    let waiting = Arc::clone(&volumes);
    let first = std::thread::spawn(move || waiting.sized(FIRST));
    shelf.update(|state| {
        state.volumes.insert(FIRST.0);
        shelf.kept(state, FIRST.0).unwrap().sized = Some(64);
    });
    assert_eq!(first.join().unwrap().unwrap(), Some(64));
    shelf.update(|state| state.complete = true);
    assert_eq!(volumes.sized(VolumeIndex(3)).unwrap(), None);
}

#[test]
fn a_read_of_bytes_nothing_will_bring_fails_as_lost() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"0123", Provenance::Fresh)
        .unwrap();
    let volumes = reader(&shelf);
    // The read waits while the volume can still grow, and fails once it
    // settles short of the range.
    let waiting = Arc::clone(&volumes);
    let first = std::thread::spawn(move || read(&waiting, 2, 4));
    close(&shelf, 0, 10, true);
    assert_eq!(first.join().unwrap().unwrap(), b"23");
    let error = read(&volumes, 4, 2).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert_eq!(
        Lost::of(&error),
        Some(Lost {
            volume: FIRST,
            range: ByteRange::new(4, 6)
        })
    );
    assert!(volumes.lost());
}

// Every volume a run reads is in once no volume can join and each bound one
// has closed: until then a member it finds may still wait on bytes.
#[test]
fn a_run_has_all_its_volumes_once_none_can_join_and_each_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (_envelopes, shelf) = shelved(dir.path());
    let volumes = reader(&shelf);
    close(&shelf, 0, 10, false);
    assert!(!volumes.all_in(), "another volume can still join");
    shelf.update(|state| {
        state.volumes.insert(1);
        state.complete = true;
    });
    assert!(!volumes.all_in(), "the second volume is still open");
    close(&shelf, 1, 4, false);
    assert!(volumes.all_in());
    shelf.update(|state| state.deciding = true);
    assert!(!volumes.all_in(), "a member beside them is still deciding");
}

#[test]
fn a_volume_that_never_joined_is_lost_once_no_more_can_join() {
    let dir = tempfile::tempdir().unwrap();
    let (_envelopes, shelf) = shelved(dir.path());
    shelf.update(|state| {
        state.volumes = std::collections::BTreeSet::from([0]);
        state.complete = true;
    });
    let volumes = reader(&shelf);
    let mut into = [0; 4];
    let error = volumes.read_at(VolumeIndex(3), 0, &mut into).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[test]
fn copies_and_growth_leave_the_run_that_observed_the_bytes_current() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"original", Provenance::Fresh)
        .unwrap();
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 0, 8).unwrap(), b"original");
    envelopes
        .keep(NODE, 0, 0, b"original", Provenance::Fresh)
        .unwrap();
    assert!(
        volumes.current(),
        "an identical copy changes no observed input"
    );
    envelopes
        .keep(NODE, 0, 8, b"tail", Provenance::Fresh)
        .unwrap();
    assert!(volumes.current(), "unread input remains ordinary growth");
    envelopes
        .keep(NODE, 0, 7, b"lTAIL", Provenance::Fresh)
        .unwrap();
    assert!(
        volumes.current(),
        "a partial overlap preserves every consumed byte"
    );
}

#[test]
fn a_first_write_below_an_observed_footer_is_not_a_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 8, b"tail", Provenance::Fresh)
        .unwrap();
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 8, 4).unwrap(), b"tail");
    envelopes
        .keep(NODE, 0, 0, b"original", Provenance::Fresh)
        .unwrap();
    assert!(
        volumes.current(),
        "the high-water mark does not make a sparse hole observed input"
    );
    assert_eq!(read(&volumes, 0, 12).unwrap(), b"originaltail");
}

#[test]
fn a_repair_behind_what_the_run_read_stops_its_reads() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"01234567", Provenance::Fresh)
        .unwrap();
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 0, 8).unwrap(), b"01234567");
    // A repair ahead of the mark is what the run will read anyway.
    envelopes
        .keep(NODE, 0, 8, b"89", Provenance::Repaired)
        .unwrap();
    assert!(volumes.current());
    envelopes
        .keep(NODE, 0, 2, b"xy", Provenance::Repaired)
        .unwrap();
    assert!(!volumes.current());
    let error = read(&volumes, 8, 2).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

#[test]
fn releasing_envelope_handles_stops_old_readers_without_changing_bytes_or_facts() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"front", Provenance::Fresh)
        .unwrap();
    close(&shelf, 0, 8, false);
    let files = envelopes.open_files();
    assert_eq!(files.len(), 1);
    let superseded = reader(&shelf);
    assert_eq!(read(&superseded, 0, 5).unwrap(), b"front");
    shelf.restart();
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 0, 5).unwrap(), b"front");
    assert_eq!(superseded.open_files(), 1);
    assert_eq!(volumes.open_files(), 1);
    // Whether the thread has entered its wait yet or not, the handoff
    // invalidates this generation and it must never reopen the file.
    let waiting_volumes = Arc::clone(&volumes);
    let waiting = std::thread::spawn(move || read(&waiting_volumes, 5, 3));
    envelopes.release_handles();
    assert!(envelopes.open_files().is_empty());
    assert_eq!(superseded.open_files(), 0);
    assert_eq!(volumes.open_files(), 0);
    assert_eq!(
        read(&volumes, 0, 5).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(volumes.open_files(), 0);
    assert_eq!(
        waiting.join().unwrap().unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert_eq!(std::fs::read(&files[0]).unwrap(), b"front");
    let state = shelf.lock();
    let kept = &state.kept[&0];
    assert_eq!(kept.closed, Some(8));
    assert_eq!(kept.coverage.bytes_covered(), 5);
}

#[test]
fn forgetting_the_node_stops_a_waiting_read() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes.keep(NODE, 0, 0, b"0", Provenance::Fresh).unwrap();
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 0, 1).unwrap(), b"0");
    assert_eq!(volumes.open_files(), 1);
    let waiting_volumes = Arc::clone(&volumes);
    let read = std::thread::spawn(move || read(&waiting_volumes, 0, 4));
    let files = envelopes.forget(NODE);
    assert_eq!(volumes.open_files(), 0);
    let error = read.join().unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    // The file stays for its owner to remove once no checkpoint names it.
    assert_eq!(files.len(), 1);
    assert!(files[0].path().exists());
}

#[test]
fn dropping_storage_releases_handles_even_when_a_reader_keeps_the_shelf_alive() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes
        .keep(NODE, 0, 0, b"held", Provenance::Fresh)
        .unwrap();
    let volumes = reader(&shelf);
    assert_eq!(read(&volumes, 0, 4).unwrap(), b"held");
    assert_eq!(volumes.open_files(), 1);
    let files = envelopes.open_files();
    drop(envelopes);
    assert_eq!(volumes.open_files(), 0);
    assert_eq!(
        read(&volumes, 0, 4).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    // On NFS an unlinked open handle would leave a silly-rename entry.
    std::fs::remove_file(&files[0]).unwrap();
    assert_eq!(
        std::fs::read_dir(files[0].parent().unwrap())
            .unwrap()
            .count(),
        0
    );
}

// A run that says nothing and returns.
struct Quiet;

impl Extraction for Quiet {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, _: &mut Sink<'_>, _: weaver_graph::Budget) {}
}

// A decode pool with no limit, and one of `limit` bytes.
fn unbounded() -> Arc<DecodePool> {
    bounded(u64::MAX)
}

fn bounded(limit: u64) -> Arc<DecodePool> {
    Arc::new(DecodePool::new(limit))
}

// Holds that may keep `resident_limit` bytes resident, and page any more.
fn holds(resident_limit: u64) -> Arc<HoldsAccountant> {
    Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }))
}

#[tokio::test]
async fn a_run_takes_a_slot_and_the_next_queues_behind_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), unbounded());
    let (first, second) = (NodeId(1), NodeId(2));
    assert_eq!(runs.ask(first, 1), vec![first]);
    assert!(
        runs.ask(second, 1).is_empty(),
        "one slot, and the first holds it"
    );
    runs.start(first, Box::new(Quiet), envelopes.shelf(first));
    assert!(runs.live());

    let said = mail.recv().await.unwrap();
    assert_eq!(said.node, first);
    assert!(matches!(said.heard, Heard::Ended));
    assert_eq!(runs.ended(first, said.generation, 1), vec![second]);
    assert!(runs.live(), "the second holds the slot and has yet to run");

    runs.start(second, Box::new(Quiet), envelopes.shelf(second));
    let said = mail.recv().await.unwrap();
    assert_eq!(said.node, second);
    assert!(runs.ended(second, said.generation, 1).is_empty());
    assert!(!runs.live());
}

// A run that panics as it reads.
struct Panics;

impl Extraction for Panics {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, _: &mut Sink<'_>, _: weaver_graph::Budget) {
        panic!("the run panics as it reads");
    }
}

// A run that panics still ends: its end is said as its thread unwinds, and
// gives its slot to the run waiting behind it.
#[tokio::test]
async fn a_run_that_panics_still_says_it_ended() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), unbounded());
    let (first, second) = (NodeId(1), NodeId(2));
    assert_eq!(runs.ask(first, 1), vec![first]);
    assert!(runs.ask(second, 1).is_empty());
    runs.start(first, Box::new(Panics), envelopes.shelf(first));

    let said = mail.recv().await.unwrap();
    assert_eq!(said.node, first);
    assert!(matches!(said.heard, Heard::Ended));
    assert_eq!(runs.ended(first, said.generation, 1), vec![second]);
}

// A run that waits for its first byte, then decodes until it is told to
// stop: it says it is decoding as it begins.
struct Decodes {
    decoding: std::sync::mpsc::Sender<()>,
    done: std::sync::mpsc::Receiver<()>,
}

impl Extraction for Decodes {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, _: &mut Sink<'_>, _: weaver_graph::Budget) {
        let mut byte = [0; 1];
        volumes.read_at(FIRST, 0, &mut byte).unwrap();
        self.decoding.send(()).unwrap();
        let _ = self.done.recv();
    }
}

// Until no decoder executes, having executed `after` times. A decoder's
// thread does not need the test's runtime to get there.
fn idle_after(clock: &crate::pipeline::graph::decoding::DecodeClock, after: u64) {
    while !clock.idle_after(after) {
        std::thread::yield_now();
    }
}

// The deadline counts a decoder executing, and nothing a run waits for:
// not a slot, not its admission before it starts, not its bytes, and not
// the time after it returned.
#[tokio::test(start_paused = true)]
async fn a_run_spends_only_the_time_its_decoder_executes() {
    use crate::pipeline::graph::decoding::DecodeClock;
    use std::time::Duration;

    const PAST: Duration = Duration::from_secs(100);
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let mut mail = RunMail::default();
    let clock = DecodeClock::default();
    let mut runs = Runs::new(1, 1, mail.sender(), unbounded()).timed(clock.clone());
    let (first, second) = (NodeId(1), NodeId(2));
    assert_eq!(runs.ask(first, 1), vec![first]);
    assert!(
        runs.ask(second, 1).is_empty(),
        "the second waits for a slot"
    );
    tokio::time::advance(PAST).await;
    assert_eq!(
        clock.spent(),
        Duration::ZERO,
        "admitted, queued, not started"
    );

    let (decoding, decoded) = std::sync::mpsc::channel();
    let (finish, done) = std::sync::mpsc::channel();
    runs.start(
        first,
        Box::new(Decodes { decoding, done }),
        envelopes.shelf(first),
    );
    idle_after(&clock, 1);
    tokio::time::advance(PAST).await;
    assert_eq!(clock.spent(), Duration::ZERO, "waiting for its first byte");

    envelopes
        .keep(first, 0, 0, b"x", Provenance::Fresh)
        .unwrap();
    decoded.recv().unwrap();
    tokio::time::advance(Duration::from_secs(61)).await;
    assert_eq!(clock.spent(), Duration::from_secs(61), "decoding");
    finish.send(()).unwrap();
    let said = mail.recv().await.unwrap();
    assert!(matches!(said.heard, Heard::Ended));
    idle_after(&clock, 2);
    tokio::time::advance(PAST).await;
    assert_eq!(clock.spent(), Duration::from_secs(61), "returned");
}

// A run turned away for memory spends nothing while it waits for it.
#[tokio::test(start_paused = true)]
async fn a_run_starved_of_memory_spends_nothing() {
    use crate::pipeline::graph::decoding::DecodeClock;
    use std::time::Duration;

    const WINDOW: u64 = 1 << 20;
    let clock = DecodeClock::default();
    let mut runs = Runs::new(
        1,
        2,
        RunMail::default().sender(),
        bounded(WINDOW + RUN_OVERHEAD),
    )
    .timed(clock.clone());
    let (first, second) = (NodeId(1), NodeId(2));
    assert_eq!(runs.ask(first, 2), vec![first]);
    assert_eq!(runs.reserve(first, WINDOW), Reservation::Granted);
    assert_eq!(runs.ask(second, 2), vec![second]);
    assert_eq!(
        runs.reserve(second, WINDOW),
        Reservation::Starved(Vec::new())
    );
    tokio::time::advance(Duration::from_secs(100)).await;
    assert_eq!(clock.spent(), Duration::ZERO);
}

// A run stopped as the job goes down keeps the generation it ran under, so
// no run starts on its node under that generation again: what its thread
// says once stopped, its end included, is not heard as any run's. It gives
// back its slot, and the job is not quiet, since it never finished reading.
#[tokio::test]
async fn a_stopped_run_keeps_its_generation_and_what_it_says_after_is_not_heard() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), unbounded());
    let (first, second) = (NodeId(1), NodeId(2));
    assert_eq!(runs.ask(first, 1), vec![first]);
    let shelf = envelopes.shelf(first);
    runs.start(first, Box::new(Quiet), Arc::clone(&shelf));
    let generation = runs.generation(first).expect("the run started");

    runs.stop(1);
    assert!(runs.has(first), "the stopped run stays");
    assert_eq!(runs.generation(first), Some(generation));
    assert_eq!(shelf.generation(), generation, "no step retracts it");
    assert!(!runs.in_flight(), "nothing of it is awaited");
    assert!(runs.live(), "it never finished reading");
    assert_eq!(runs.ask(second, 1), vec![second], "its slot is free");

    let said = mail.recv().await.unwrap();
    assert_eq!((said.node, said.generation), (first, generation));
    assert!(matches!(said.heard, Heard::Ended));
    assert!(!runs.current(first, said.generation));
}

// Three runs hold the three slots and a fourth waits. The limit falls to
// one: no holder is stopped, the first two to end admit no one, and the
// third to end admits the waiter, so the job is never left holding fewer
// runs than the limit allows while one waits.
#[test]
fn lowering_the_limit_wakes_the_next_waiter_with_no_held_child_left_behind() {
    let mut runs = Runs::new(1, 3, RunMail::default().sender(), unbounded());
    let nodes: Vec<NodeId> = (1..=4).map(NodeId).collect();
    for node in &nodes[..3] {
        assert_eq!(runs.ask(*node, 3), vec![*node]);
    }
    assert!(runs.ask(nodes[3], 3).is_empty(), "the fourth waits");

    assert!(runs.ended(nodes[0], 0, 1).is_empty(), "two still hold");
    assert!(runs.ended(nodes[1], 0, 1).is_empty(), "one still holds");
    assert_eq!(
        runs.ended(nodes[2], 0, 1),
        vec![nodes[3]],
        "the last holder's release is the first under the new limit"
    );
    assert_eq!(runs.holding(), vec![nodes[3]]);
    assert!(runs.ended(nodes[3], 0, 1).is_empty());
    assert!(runs.holding().is_empty());
    assert!(!runs.live());
}

// A raised limit admits the waiters at once, handed back with the asker's
// own admission so the pipeline starts them in the same step.
#[test]
fn raising_the_limit_admits_the_waiters_in_the_same_step() {
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), unbounded());
    let nodes: Vec<NodeId> = (1..=4).map(NodeId).collect();
    assert_eq!(runs.ask(nodes[0], 1), vec![nodes[0]]);
    assert!(runs.ask(nodes[1], 1).is_empty());
    assert!(runs.ask(nodes[2], 1).is_empty());
    assert_eq!(runs.ask(nodes[3], 4), vec![nodes[1], nodes[2], nodes[3]]);
    assert_eq!(runs.holding(), nodes);
}

// The reservation is the declared window and the fixed overhead, exactly,
// both on the run and in the decode pool. Asking again with a wider window,
// as a restart would, takes nothing more; the end gives back all of it.
#[test]
fn memory_reserved_is_the_declared_window_plus_the_fixed_overhead_and_never_grows() {
    const WINDOW: u64 = 3 << 20;
    let pool = unbounded();
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool));
    let node = NodeId(5);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, WINDOW), Reservation::Granted);
    assert_eq!(runs.reserved(node), WINDOW + RUN_OVERHEAD);
    assert_eq!(pool.room().reserved, WINDOW + RUN_OVERHEAD);

    assert_eq!(runs.reserve(node, 4 * WINDOW), Reservation::Granted);
    assert_eq!(runs.reserved(node), WINDOW + RUN_OVERHEAD, "it never grows");
    assert_eq!(pool.room().reserved, WINDOW + RUN_OVERHEAD);

    runs.ended(node, 0, 1);
    assert_eq!(runs.reserved(node), 0);
    assert_eq!(pool.room().reserved, 0);
}

// A reservation that does not fit beside what the pool holds gives the slot
// to the next waiter and waits; once the pool is given bytes back it asks
// again, through the same release that frees a slot.
#[test]
fn a_window_that_does_not_fit_yields_its_slot_and_asks_again_once_memory_is_released() {
    const WINDOW: u64 = 1 << 20;
    let pool = bounded(2 * (WINDOW + RUN_OVERHEAD));
    let mut runs = Runs::new(1, 2, RunMail::default().sender(), pool);
    let (first, second, third) = (NodeId(1), NodeId(2), NodeId(3));
    assert_eq!(runs.ask(first, 2), vec![first]);
    assert_eq!(runs.reserve(first, WINDOW), Reservation::Granted);
    assert_eq!(runs.ask(second, 2), vec![second]);
    assert!(runs.ask(third, 2).is_empty(), "the third waits for a slot");
    // The second wants a byte more than is left.
    assert_eq!(
        runs.reserve(second, WINDOW + 1),
        Reservation::Starved(vec![third]),
        "the second's slot goes to the third"
    );
    assert_eq!(runs.holding(), vec![first, third]);
    assert_eq!(runs.reserved(second), 0);
    assert!(runs.live(), "a starved run is still the job's to finish");

    // The first's end releases its reservation: the second asks again and
    // takes the slot the first held.
    assert_eq!(runs.ended(first, 0, 2), vec![second]);
    assert_eq!(runs.reserve(second, WINDOW + 1), Reservation::Granted);
    assert_eq!(runs.reserved(second), WINDOW + 1 + RUN_OVERHEAD);
}

// A reservation larger than the decode pool itself can never fit.
#[test]
fn a_window_larger_than_the_decode_pool_is_refused() {
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), bounded(RUN_OVERHEAD));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(
        runs.reserve(node, 1),
        Reservation::Refused {
            needed: 1 + RUN_OVERHEAD,
            allowed: RUN_OVERHEAD
        }
    );
    assert_eq!(runs.reserved(node), 0);
}

// A graph job of echo volumes, one article each, read by echo engines.
struct Echoing {
    job: GraphJob,
    mail: RunMail,
    bids: Arc<Mutex<Vec<Seek>>>,
    volumes: Vec<Vec<u8>>,
    dir: tempfile::TempDir,
    pool: Arc<DecodePool>,
    holds: Arc<HoldsAccountant>,
}

// The body of volume `index`: bytes no format sniffs as its own, a length
// that ends a read short.
fn body(index: u32) -> Vec<u8> {
    (0..40u8)
        .map(|byte| byte.wrapping_mul(7).wrapping_add(index as u8) | 0x80)
        .collect()
}

fn echoing(count: u32) -> Echoing {
    echoing_within(count, 0, unbounded())
}

// Echo volumes whose engine declares `window`, reserved from `pool`.
fn echoing_within(count: u32, window: u64, pool: Arc<DecodePool>) -> Echoing {
    echoing_in(1, count, window, pool, holds(u64::MAX))
}

// The same, as job `scope`, holding its members under `holds`: jobs that
// share `pool` and `holds` share both.
fn echoing_in(
    scope: u64,
    count: u32,
    window: u64,
    pool: Arc<DecodePool>,
    holds: Arc<HoldsAccountant>,
) -> Echoing {
    echoing_scoped(
        scope,
        Echoes {
            volumes: count,
            window,
            ..Echoes::default()
        },
        pool,
        holds,
    )
}

fn echoing_as(echoes: Echoes, pool: Arc<DecodePool>) -> Echoing {
    echoing_scoped(1, echoes, pool, holds(u64::MAX))
}

fn echoing_scoped(
    scope: u64,
    echoes: Echoes,
    pool: Arc<DecodePool>,
    holds: Arc<HoldsAccountant>,
) -> Echoing {
    let count = echoes.volumes;
    let volumes: Vec<Vec<u8>> = (0..count)
        .map(|index| {
            let mut volume = if index == 0 {
                SIGNATURE.to_vec()
            } else {
                vec![0x80; HEADER]
            };
            volume.extend(body(index));
            volume
        })
        .collect();
    let files: Vec<RootFile> = volumes
        .iter()
        .enumerate()
        .map(|(index, volume)| RootFile {
            name: format!("lantern.part{}.rar", index + 1),
            articles: vec![(0, volume.len() as u32)],
        })
        .collect();
    let bids = Arc::clone(&echoes.bids);
    let dir = tempfile::tempdir().unwrap();
    let writer = NamespaceWriter::new(
        scope,
        dir.path().to_path_buf(),
        Arc::clone(&holds),
        CapacityReader::unknown(),
    );
    let mail = RunMail::default();
    let runs = Runs::new(scope, 1, mail.sender(), Arc::clone(&pool));
    let job = GraphJob::new(&files, Box::new(echoes), writer, runs, u32::MAX);
    Echoing {
        job,
        mail,
        bids,
        volumes,
        dir,
        pool,
        holds,
    }
}

impl Echoing {
    fn deliver(&mut self, volume: u32) {
        let bytes = &self.volumes[volume as usize];
        self.job
            .intake(
                Decoded {
                    volume: VolumeIndex(volume),
                    ordinal: 0,
                    offset: 0,
                    bytes: Bytes::copy_from_slice(bytes),
                    intact: true,
                    positioned: true,
                    length: Some(bytes.len() as u64),
                    declared_end: None,
                    segments: Vec::new(),
                },
                &mut RepairSlots::default(),
                1,
            )
            .unwrap();
    }

    fn engine(&self) -> NodeId {
        let engines = self.job.tree().engines();
        assert_eq!(engines.len(), 1, "the store was built as its engine");
        engines[0]
    }

    fn closed(&self, member: u32) -> bool {
        self.job
            .tree()
            .namespace(self.engine())
            .and_then(|namespace| namespace.get(MemberId(member)))
            .is_some_and(|record| record.closed)
    }

    // Hear one thing the runs said. True when it was the current run
    // returning.
    async fn hear(&mut self) -> bool {
        let said = self.mail.recv().await.unwrap();
        let ended = matches!(said.heard, Heard::Ended)
            && self.job.runs().current(said.node, said.generation);
        self.job
            .run_said(said, &mut RepairSlots::default(), 1)
            .unwrap();
        ended
    }

    async fn until_closed(&mut self, member: u32) {
        while !self.closed(member) {
            self.hear().await;
        }
    }

    fn bid(&mut self) -> Vec<Seek> {
        self.bids.lock().unwrap().clear();
        self.job
            .seek_round(|_| true, &mut RepairSlots::default(), 1)
            .unwrap();
        std::mem::take(&mut *self.bids.lock().unwrap())
    }

    fn echoed(&self, member: u32) -> Vec<u8> {
        std::fs::read(self.dir.path().join(format!("echo-{member}.mkv"))).unwrap()
    }
}

fn whole(volume: u32, weight: u32) -> Seek {
    Seek::data(
        VolumeIndex(volume),
        ByteRange::new(0, u64::MAX),
        Weight(weight),
    )
}

#[tokio::test]
async fn the_job_reports_extracting_while_a_run_is_live_and_done_after() {
    let mut echoing = echoing(1);
    echoing.deliver(0);
    assert_eq!(echoing.job.phase(), RootPhase::Extracting);
    loop {
        let said = echoing.mail.recv().await.unwrap();
        let ended = matches!(said.heard, Heard::Ended);
        assert_eq!(echoing.job.phase(), RootPhase::Extracting);
        echoing
            .job
            .run_said(said, &mut RepairSlots::default(), 1)
            .unwrap();
        if ended {
            break;
        }
    }
    assert_eq!(echoing.job.phase(), RootPhase::Done);
    assert_eq!(echoing.echoed(0), body(0));
}

// A second clean copy of a volume, with other bytes, after the run that read
// the first ended or while it still reads: the first copy stands, so nothing
// is retracted and no run starts again.
#[tokio::test]
async fn a_different_verified_copy_leaves_a_finished_runs_output() {
    first_copy_stands(true).await;
}

#[tokio::test]
async fn a_different_verified_copy_leaves_a_live_runs_output() {
    first_copy_stands(false).await;
}

async fn first_copy_stands(ended: bool) {
    let mut echoing = echoing(1);
    echoing.deliver(0);
    if ended {
        while !echoing.hear().await {}
        assert_eq!(echoing.job.phase(), RootPhase::Done);
    } else {
        echoing.until_closed(0).await;
    }
    let engine = echoing.engine();
    let generation = echoing.job.runs().generation(engine).unwrap();
    for byte in &mut echoing.volumes[0][HEADER..] {
        *byte ^= 0x5a;
    }
    echoing.deliver(0);
    assert!(echoing.closed(0), "the first copy's output stands");
    assert_eq!(echoing.job.runs().generation(engine), Some(generation));
    if !ended {
        while !echoing.hear().await {}
    }
    assert!(!echoing.job.runs().live(), "no run reads the second copy");
    assert_eq!(echoing.echoed(0), body(0));
    assert_eq!(echoing.job.phase(), RootPhase::Done);
}

// Two clean copies of one volume that differ, in either order, in two jobs:
// each job's output is the copy it took first, byte for byte.
#[tokio::test]
async fn differing_duplicate_copies_give_one_output_in_either_order() {
    for flipped in [false, true] {
        let mut echoing = echoing(1);
        let mut other = echoing.volumes[0].clone();
        for byte in &mut other[HEADER..] {
            *byte ^= 0x5a;
        }
        if flipped {
            other = std::mem::replace(&mut echoing.volumes[0], other);
        }
        let first = echoing.volumes[0].clone();
        echoing.deliver(0);
        echoing.volumes[0] = other;
        echoing.deliver(0);
        while !echoing.hear().await {}
        assert_eq!(echoing.echoed(0), first[HEADER..], "flipped: {flipped}");
        assert_eq!(echoing.job.phase(), RootPhase::Done);
    }
}

// A volume that came before the one that built its engine was a file of its
// own. The engine keeps the volume whole, so its envelope is that file under
// a second name, not a copy, and the first name goes once the checkpoint
// that stops naming it commits.
#[tokio::test]
async fn a_volume_its_engine_takes_from_its_file_is_on_disk_once() {
    let mut echoing = echoing(2);
    echoing.deliver(1);
    let posted = echoing.dir.path().join("lantern.part2.rar");
    assert_eq!(std::fs::read(&posted).unwrap(), echoing.volumes[1]);
    echoing.deliver(0);
    let envelope = echoing.dir.path().join(envelope_name(echoing.engine(), 1));
    assert_eq!(std::fs::read(&envelope).unwrap(), echoing.volumes[1]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(&posted).unwrap().ino(),
            std::fs::metadata(&envelope).unwrap().ino(),
            "the envelope is the volume's file"
        );
    }
    echoing.job.recorded(1);
    echoing.job.committed(1).unwrap();
    assert!(!posted.exists());
    assert_eq!(std::fs::read(&envelope).unwrap(), echoing.volumes[1]);
    while !echoing.hear().await {}
    for member in 0..2 {
        assert_eq!(echoing.echoed(member), body(member));
    }
}

#[tokio::test]
async fn the_engine_bids_for_the_volumes_after_the_one_its_run_reached() {
    let mut echoing = echoing(3);
    assert!(echoing.bid().is_empty(), "nothing is bound yet");
    echoing.deliver(0);
    echoing.until_closed(0).await;
    assert_eq!(echoing.bid(), vec![whole(1, 1), whole(2, 2)]);

    echoing.deliver(1);
    echoing.until_closed(1).await;
    assert_eq!(echoing.bid(), vec![whole(2, 1)]);

    echoing.deliver(2);
    while !echoing.hear().await {}
    for member in 0..3 {
        assert_eq!(echoing.echoed(member), body(member));
    }
    assert_eq!(echoing.job.phase(), RootPhase::Done);
}

// The job reserves the engine's declared window and the overhead before
// its run starts, holds exactly that while it runs, and gives it back when
// the pipeline hears the run end.
#[tokio::test]
async fn a_job_holds_its_engines_window_while_the_run_is_live() {
    const WINDOW: u64 = 5 << 20;
    let mut echoing = echoing_within(1, WINDOW, unbounded());
    echoing.deliver(0);
    let engine = echoing.engine();
    assert_eq!(echoing.job.runs().reserved(engine), WINDOW + RUN_OVERHEAD);
    assert_eq!(echoing.pool.room().reserved, WINDOW + RUN_OVERHEAD);
    assert_eq!(
        echoing
            .holds
            .namespace_usage(HoldsNamespace {
                scope: 1,
                node: engine
            })
            .resident,
        0,
        "the window is the decode pool's, never the holds'"
    );
    while !echoing.hear().await {}
    assert_eq!(echoing.echoed(0), body(0));
    assert_eq!(echoing.pool.room().reserved, 0);
}

// An engine whose window can never fit is refused to the tree as too large,
// with what it needs against the decode pool, and its volume is a leaf that
// says why.
#[tokio::test]
async fn an_engine_whose_window_can_never_fit_is_refused_too_large() {
    let mut echoing = echoing_within(1, 1 << 30, bounded(64 << 20));
    echoing.deliver(0);
    assert!(echoing.job.tree().engines().is_empty());
    assert_eq!(
        echoing.job.writer().refused(NodeId::ROOT, MemberId(0)),
        Some(Why::TooLarge {
            needed: Some((1 << 30) + RUN_OVERHEAD),
            allowed: Some(64 << 20),
        })
    );
    assert!(!echoing.job.runs().live());
    assert_eq!(echoing.pool.room().reserved, 0);
}

// The holds keep every byte they may keep resident when the engine wants to
// start: its run reserves from the decode pool, which the holds never touch,
// so it starts at once, and its members are written past the full holds.
#[tokio::test]
async fn a_decode_at_the_ceiling_runs_while_the_holds_are_full() {
    const WINDOW: u64 = 1 << 20;
    const HELD: u64 = 64 << 10;
    let full = holds(HELD);
    let other = HoldsNamespace {
        scope: 2,
        node: NodeId::ROOT,
    };
    assert_eq!(
        full.reserve_resident(other, HELD),
        weaver_graph::holds::Reserve::Granted
    );
    let pool = bounded(WINDOW + RUN_OVERHEAD);
    let mut echoing = echoing_in(1, 2, WINDOW, Arc::clone(&pool), Arc::clone(&full));
    echoing.deliver(0);
    let engine = echoing.engine();
    assert_eq!(echoing.job.runs().reserved(engine), WINDOW + RUN_OVERHEAD);
    assert_eq!(full.usage().resident, HELD, "the holds gave nothing up");
    echoing.deliver(1);
    while !echoing.hear().await {}
    for member in 0..2 {
        assert_eq!(echoing.echoed(member), body(member));
    }
    assert_eq!(pool.room().reserved, 0);
}

// Two jobs share a pool their runs do not fit in together. The second's run
// waits, holding no slot and no memory, until the first's end frees the
// pool; the pipeline then steps it again, it starts, and both complete.
// Bytes the holds take or give back meanwhile neither keep it waiting nor
// wake it.
#[tokio::test]
async fn two_runs_that_do_not_fit_together_run_one_after_the_other() {
    const WINDOW: u64 = 1 << 20;
    let pool = bounded(WINDOW + RUN_OVERHEAD);
    let shared = holds(u64::MAX);
    let mut first = echoing_in(1, 1, WINDOW, Arc::clone(&pool), Arc::clone(&shared));
    let mut second = echoing_in(2, 1, WINDOW, Arc::clone(&pool), Arc::clone(&shared));
    first.deliver(0);
    second.deliver(0);
    let engine = second.engine();
    assert_eq!(second.job.runs().reserved(engine), 0, "it did not fit");
    assert!(second.job.runs().holding().is_empty(), "nor holds a slot");
    assert!(second.job.runs().live(), "it waits for memory");
    assert!(!second.job.memory_freed(), "nothing was freed yet");
    let other = HoldsNamespace {
        scope: 3,
        node: NodeId::ROOT,
    };
    assert_eq!(
        shared.reserve_resident(other, WINDOW),
        weaver_graph::holds::Reserve::Granted
    );
    shared.release_resident(other, WINDOW);
    assert!(
        !second.job.memory_freed(),
        "held bytes given back are not decode memory"
    );

    while !first.hear().await {}
    assert_eq!(first.echoed(0), body(0));
    assert!(second.job.memory_freed(), "the first's end freed the pool");
    second
        .job
        .resume_runs(&mut RepairSlots::default(), 1)
        .unwrap();
    assert_eq!(second.job.runs().reserved(engine), WINDOW + RUN_OVERHEAD);
    while !second.hear().await {}
    assert_eq!(second.echoed(0), body(0));
    assert_eq!(second.job.phase(), RootPhase::Done);
    assert_eq!(pool.room().reserved, 0);
}

// A job cancelled while its run waits for memory has nothing out on a
// thread, so the pipeline lets it go at once, and its waiter with it: the
// pool's release after wakes nothing of it.
#[tokio::test]
async fn a_run_waiting_for_memory_goes_with_its_cancelled_job() {
    const WINDOW: u64 = 1 << 20;
    let pool = bounded(WINDOW + RUN_OVERHEAD);
    let shared = holds(u64::MAX);
    let mut first = echoing_in(1, 1, WINDOW, Arc::clone(&pool), Arc::clone(&shared));
    let mut second = echoing_in(2, 1, WINDOW, Arc::clone(&pool), Arc::clone(&shared));
    first.deliver(0);
    second.deliver(0);
    assert!(second.job.runs().live(), "it waits for memory");
    second.job.request_stop();
    assert!(!second.job.awaits_mail(), "nothing of it is out");
    drop(second);
    while !first.hear().await {}
    assert_eq!(pool.room().reserved, 0);
    assert_eq!(shared.usage().resident, 0);
    assert_eq!(shared.usage().namespaces, 0);
}

// A job stopped while its run waits for memory stops the waiter where it
// stands: no release wakes it, and it holds nothing.
#[test]
fn a_run_waiting_for_memory_is_stopped_with_its_job() {
    const WINDOW: u64 = 1 << 20;
    let pool = bounded(WINDOW + RUN_OVERHEAD);
    let mut first = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool));
    let mut second = Runs::new(2, 1, RunMail::default().sender(), Arc::clone(&pool));
    let node = NodeId(1);
    assert_eq!(first.ask(node, 1), vec![node]);
    assert_eq!(first.reserve(node, WINDOW), Reservation::Granted);
    assert_eq!(second.ask(node, 1), vec![node]);
    assert_eq!(
        second.reserve(node, WINDOW),
        Reservation::Starved(Vec::new())
    );
    second.stop(1);
    first.ended(node, 0, 1);
    assert_eq!(pool.room().reserved, 0);
    assert!(!second.wakeable(), "a stopped waiter is not woken");
    assert!(second.forget(Vec::new(), 1).is_empty());
    assert_eq!(second.reserved(node), 0);
}

// A raised pool wakes a run turned away for memory, as a release does.
#[test]
fn a_raised_pool_wakes_a_run_turned_away_for_memory() {
    const WINDOW: u64 = 1 << 20;
    let pool = bounded(WINDOW + RUN_OVERHEAD);
    let mut other = Runs::new(2, 1, RunMail::default().sender(), Arc::clone(&pool));
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool));
    let node = NodeId(1);
    assert_eq!(other.ask(node, 1), vec![node]);
    assert_eq!(other.reserve(node, 0), Reservation::Granted);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, WINDOW), Reservation::Starved(Vec::new()));
    assert!(!runs.wakeable());
    pool.set_limit(WINDOW + 2 * RUN_OVERHEAD);
    assert!(runs.wakeable());
    assert_eq!(runs.forget(Vec::new(), 1), vec![node]);
    assert_eq!(runs.reserve(node, WINDOW), Reservation::Granted);
}

// A posted volume the run already read closes again at another length, as
// a recovery set's description corrects it: every offset after it moved, so
// the run built on the old ones starts over.
#[tokio::test]
async fn a_volume_the_run_read_closing_at_another_length_restarts_the_run() {
    let mut echoing = echoing(3);
    echoing.deliver(0);
    echoing.until_closed(0).await;
    assert_eq!(echoing.echoed(0), body(0));
    let engine = echoing.engine();
    let before = echoing.job.runs().generation(engine);
    assert!(before.is_some(), "the run is live");
    let namespace = echoing.job.namespace_mut(NodeId::ROOT).unwrap();
    let name = namespace.get(MemberId(0)).unwrap().name.clone();
    let length = namespace.get(MemberId(0)).unwrap().length.unwrap();
    namespace.recognise(MemberId(0), &name, length - 1);
    echoing.deliver(1);
    assert_ne!(echoing.job.runs().generation(engine), before);
    assert!(!echoing.closed(0), "accepted old output must be retracted");
    // The retracted output stays where the last checkpoint names it until
    // the one that says it is gone commits.
    assert!(echoing.dir.path().join("echo-0.mkv").exists());
    echoing.job.recorded(1);
    echoing.job.committed(1).unwrap();
    assert!(!echoing.dir.path().join("echo-0.mkv").exists());
}

// A recovery set reopens a volume whose closed length the run read, to
// rebuild it, and closes it again at the same length: nothing the run read
// moved, so what it built stays.
#[tokio::test]
async fn a_repair_that_reopens_a_length_read_volume_keeps_the_run() {
    length_read_volume_reopened(false).await;
}

// The same volume closes again at another length: the run sized itself from
// the old one, so it starts over.
#[tokio::test]
async fn a_repair_that_changes_a_read_length_retracts_the_run() {
    length_read_volume_reopened(true).await;
}

async fn length_read_volume_reopened(changed: bool) {
    let mut echoing = echoing_as(
        Echoes {
            volumes: 3,
            lengths: true,
            ..Echoes::default()
        },
        unbounded(),
    );
    for volume in 0..3 {
        echoing.deliver(volume);
    }
    while !echoing.hear().await {}
    let engine = echoing.engine();
    let before = echoing.job.runs().generation(engine);
    let set = |echoing: &mut Echoing, closed: bool| {
        let namespace = echoing.job.namespace_mut(NodeId::ROOT).unwrap();
        if closed && changed {
            let name = namespace.get(MemberId(2)).unwrap().name.clone();
            let length = namespace.get(MemberId(2)).unwrap().length.unwrap();
            namespace.recognise(MemberId(2), &name, length - 1);
        }
        namespace.get_mut(MemberId(2)).unwrap().closed = closed;
        echoing
            .job
            .resume_runs(&mut RepairSlots::default(), 1)
            .unwrap();
    };
    set(&mut echoing, false);
    assert_eq!(
        echoing.job.runs().generation(engine),
        before,
        "a reopen moves nothing the run read"
    );
    assert!((0..3).all(|member| echoing.closed(member)));
    set(&mut echoing, true);
    if !changed {
        assert_eq!(echoing.job.runs().generation(engine), before);
        for member in 0..3 {
            assert!(echoing.closed(member), "nothing was retracted");
            assert_eq!(echoing.echoed(member), body(member));
        }
        return;
    }
    assert!(!echoing.closed(0), "every member of the run is retracted");
    assert!(echoing.dir.path().join("echo-0.mkv").exists());
    echoing.job.recorded(1);
    echoing.job.committed(1).unwrap();
    assert!(!echoing.dir.path().join("echo-0.mkv").exists());
    while !echoing.hear().await {}
    assert_ne!(echoing.job.runs().generation(engine), before);
    // The rerun's output takes a backing of its own, as any replacement does.
    let record = echoing
        .job
        .tree()
        .namespace(engine)
        .unwrap()
        .get(MemberId(2))
        .unwrap();
    assert!(record.closed);
    let file = echoing.job.writer().file_state(engine, MemberId(2));
    let name = file.on_disk.as_deref().unwrap_or(&record.output_name);
    let short = body(2);
    assert_eq!(
        std::fs::read(echoing.dir.path().join(name)).unwrap(),
        short[..short.len() - 1]
    );
}

#[test]
fn giving_back_length_observed_input_stops_its_worker_before_payload_is_read() {
    for observed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (mut envelopes, shelf) = shelved(dir.path());
        close(&shelf, 0, 6, false);
        let worker = reader(&shelf);
        if observed {
            assert_eq!(worker.len(FIRST), Some(6));
        }
        assert_eq!(shelf.reached(), None);
        envelopes.take(NODE, 0).unwrap();
        assert_eq!(worker.current(), !observed);
    }
}

#[test]
fn a_stale_length_read_cannot_register_a_new_runs_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    close(&shelf, 0, 6, false);
    let old = reader(&shelf);
    assert_eq!(old.len(FIRST), Some(6));
    shelf.restart();
    let current = reader(&shelf);
    assert_eq!(
        old.len(FIRST),
        None,
        "stale metadata cannot decide a layout"
    );
    envelopes.take(NODE, 0).unwrap();
    assert!(current.current(), "the new run observed no old length");
}

#[test]
fn giving_back_read_input_stops_its_worker_but_unread_later_input_does_not() {
    for (read, give_back, stopped) in [(0, 2, false), (2, 2, true), (2, 1, true)] {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeId(1);
        let mut envelopes = Envelopes::new(dir.path().to_path_buf());
        for volume in 0..3 {
            envelopes
                .keep(node, volume, 0, b"source", Provenance::Fresh)
                .unwrap();
        }
        let shelf = envelopes.shelf(node);
        shelf.update(|state| state.volumes.extend(0..3));
        let generation = shelf.begin();
        let worker = RunVolumes::new(Arc::clone(&shelf), generation);
        assert_eq!(
            worker.read_at(VolumeIndex(read), 0, &mut [0; 6]).unwrap(),
            6
        );
        envelopes.take(node, give_back).unwrap();
        assert_eq!(worker.current(), !stopped);
        assert_eq!(shelf.generation(), generation + u64::from(stopped));
        if stopped {
            assert_eq!(
                worker
                    .read_at(VolumeIndex(0), 0, &mut [0; 6])
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::BrokenPipe
            );
        }
    }
}

// Runs granted a 1 MiB ceiling under `over_limit`, on a machine of 64 MiB:
// a run may hold 48 MiB above the ceiling, the machine less a quarter.
fn over_limit(over_limit: crate::runtime::OverLimit) -> crate::pipeline::graph::hardware::Hardware {
    crate::pipeline::graph::hardware::Hardware::granting(crate::pipeline::graph::hardware::Grant {
        run: weaver_graph::Budget {
            memory_bytes: Some(CEILING),
            window_bytes: Some(CEILING),
            threads: 1,
            ..weaver_graph::Budget::default()
        },
        over_limit,
        machine_bytes: Some(64 << 20),
        ..Default::default()
    })
}

const CEILING: u64 = 1 << 20;
const WIDE: u64 = 4 << 20;

// The decode pool of the profile: one run at the ceiling.
fn profile_pool() -> Arc<DecodePool> {
    bounded(CEILING + RUN_OVERHEAD)
}

// Under the efficient policy a window over the ceiling is held to it, and
// a decoder that refuses it is not given more: its refusal stands.
#[tokio::test]
async fn a_window_over_the_ceiling_under_the_fail_policy_is_held_to_the_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let pool = profile_pool();
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::Fail));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, WIDE), Reservation::Granted);
    assert_eq!(runs.reserved(node), CEILING + RUN_OVERHEAD);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(!runs.widen(node, Some(WIDE), None), "the refusal stands");
    assert!(!runs.rerunning(node));
    let said = mail.recv().await.unwrap();
    runs.ended(node, said.generation, 1);
    assert_eq!(pool.room().reserved, 0);
}

// A run widened above the ceiling under scratch-then-RAM, then asked again
// once the profile holds runs to the ceiling, is held to it: it reserves
// the ceiling, and its decoder's refusal of the wider window stands, with
// the sizes it states.
#[tokio::test]
async fn a_run_widened_above_the_ceiling_is_held_to_it_once_the_policy_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let pool = profile_pool();
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(runs.widen(node, Some(WIDE), None));
    let said = mail.recv().await.unwrap();
    assert_eq!(runs.ended(node, said.generation, 1), vec![node]);

    let mut runs = runs.on(over_limit(crate::runtime::OverLimit::Fail));
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    assert_eq!(runs.reserved(node), (1 << 10) + RUN_OVERHEAD);
    assert_eq!(pool.room().above, 0);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(!runs.widen(node, Some(WIDE), None), "the refusal stands");
    // A decoder that cannot say what it needs is given no more than the
    // ceiling.
    assert!(runs.widen(node, None, Some(WIDE)));
    let said = mail.recv().await.unwrap();
    assert_eq!(runs.ended(node, said.generation, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    assert_eq!(runs.reserved(node), CEILING + RUN_OVERHEAD);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(!runs.widen(node, None, Some(WIDE)), "nothing wider to give");
    let said = mail.recv().await.unwrap();
    runs.ended(node, said.generation, 1);
    assert_eq!(pool.room().reserved, 0);
}

// A decoder that refused a window within the ceiling, wider than the one it
// was given, starts again with that window reserved whole from the pool.
// One that cannot say what it needs is given the grant's window, then the
// widest its format has: a refusal again stands.
#[tokio::test]
async fn a_run_refused_within_the_ceiling_starts_again_at_the_wider_window() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let pool = profile_pool();
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::Fail));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(runs.widen(node, Some(1 << 19), None));
    let said = mail.recv().await.unwrap();
    assert_eq!(runs.ended(node, said.generation, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    assert_eq!(runs.reserved(node), (1 << 19) + RUN_OVERHEAD);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(
        !runs.widen(node, Some(1 << 19), None),
        "no wider than it was given"
    );
    let said = mail.recv().await.unwrap();
    runs.ended(node, said.generation, 1);
    assert_eq!(pool.room().reserved, 0);

    let mut runs = runs.on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    let node = NodeId(2);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(!runs.widen(node, None, None), "nothing says how wide");
    // The grant's own window first.
    assert!(runs.widen(node, None, Some(WIDE)));
    let said = mail.recv().await.unwrap();
    assert_eq!(runs.ended(node, said.generation, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    assert_eq!(runs.reserved(node), CEILING + RUN_OVERHEAD);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    // Then the widest, above the ceiling.
    assert!(runs.widen(node, None, Some(WIDE)));
    let said = mail.recv().await.unwrap();
    assert_eq!(runs.ended(node, said.generation, 1), vec![node]);
    assert_eq!(pool.room().reserved, 0);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    assert_eq!(pool.room().above, WIDE + RUN_OVERHEAD);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(
        !runs.widen(node, None, Some(WIDE)),
        "a refusal again stands"
    );
    let said = mail.recv().await.unwrap();
    runs.ended(node, said.generation, 1);
    assert_eq!(pool.room().above, 0);
}

// Under scratch-then-RAM a run whose decoder refused a window over the
// ceiling is stopped, gives back what it reserved, and starts again under
// the slot it holds, reserving the wider window in RAM above the ceiling,
// apart from the pool. A refusal no wider than what it was given then
// stands.
#[tokio::test]
async fn a_run_refused_over_the_ceiling_starts_again_at_the_wider_window_in_ram() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let pool = profile_pool();
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(runs.widen(node, Some(WIDE), None));
    assert!(runs.rerunning(node));
    let said = mail.recv().await.unwrap();
    assert!(matches!(said.heard, Heard::Ended));
    assert_eq!(
        runs.ended(node, said.generation, 1),
        vec![node],
        "it starts again under its slot"
    );
    assert!(!runs.rerunning(node));
    assert_eq!(pool.room().reserved, 0);

    assert_eq!(runs.reserve(node, 1 << 10), Reservation::Granted);
    assert_eq!(runs.reserved(node), WIDE + RUN_OVERHEAD);
    assert_eq!(pool.room().reserved, 0);
    assert_eq!(pool.room().above, WIDE + RUN_OVERHEAD);
    runs.start(node, Box::new(Quiet), envelopes.shelf(node));
    assert!(
        !runs.widen(node, Some(WIDE), None),
        "no wider than it was given"
    );
    let said = mail.recv().await.unwrap();
    runs.ended(node, said.generation, 1);
    assert_eq!(pool.room().above, 0);
}

// A window declared over the ceiling is reserved wide from the start.
#[test]
fn a_window_declared_over_the_ceiling_is_reserved_whole_in_ram() {
    let pool = profile_pool();
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, WIDE), Reservation::Granted);
    assert_eq!(runs.reserved(node), WIDE + RUN_OVERHEAD);
}

// A window the machine cannot hold beside a quarter of its memory is past
// the hard stop: 60 MiB of 64 MiB.
#[test]
fn a_window_past_what_the_machine_leaves_a_run_is_beyond_it() {
    let pool = profile_pool();
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(
        runs.reserve(node, 60 << 20),
        Reservation::Beyond {
            needed: 60 << 20,
            memory: 64 << 20
        }
    );
    assert_eq!(runs.reserved(node), 0);
    assert_eq!(pool.room().reserved, 0);
    // A window that fits under the hard stop beside the whole pool is
    // granted: 39 MiB, its overhead and the 5 MiB pool make 48 MiB. One a
    // MiB wider is beyond it.
    let (narrower, fits) = (NodeId(2), NodeId(3));
    let mut runs = Runs::new(1, 2, RunMail::default().sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    assert_eq!(runs.ask(narrower, 2), vec![narrower]);
    assert_eq!(
        runs.reserve(narrower, 40 << 20),
        Reservation::Beyond {
            needed: 40 << 20,
            memory: 64 << 20
        }
    );
    assert_eq!(runs.ask(fits, 2), vec![fits]);
    assert_eq!(runs.reserve(fits, 39 << 20), Reservation::Granted);
    assert_eq!(pool.room().above, (39 << 20) + RUN_OVERHEAD);
    drop(runs);
    assert_eq!(pool.room().above, 0, "a job dropped gives it back");
}

// A wide run starts at once beside the pool's runs, which go on sharing
// the pool beside it, in its job and in every other. One that does not fit
// under the hard stop beside the wide runs already holding waits for them,
// holding no run within the pool back. Every run finishes, and every
// reservation is given back once.
#[test]
fn a_wide_run_starts_beside_the_pools_runs_and_waits_only_on_wide_runs() {
    let pool = profile_pool();
    let mut runs = Runs::new(1, 3, RunMail::default().sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    let mut other = Runs::new(2, 2, RunMail::default().sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
    let (holder, wide, waiter) = (NodeId(1), NodeId(2), NodeId(3));
    assert_eq!(runs.ask(holder, 3), vec![holder]);
    assert_eq!(runs.reserve(holder, CEILING), Reservation::Granted);
    assert_eq!(runs.ask(wide, 3), vec![wide]);
    assert_eq!(runs.reserve(wide, 30 << 20), Reservation::Granted);
    // 5 MiB of pool, 34 MiB held above it, and 16 MiB more is over 48.
    assert_eq!(runs.ask(waiter, 3), vec![waiter]);
    assert_eq!(
        runs.reserve(waiter, 12 << 20),
        Reservation::Starved(Vec::new())
    );
    // The pool's runs, in this job and another, are held back by none of
    // it, and their ends make no room for it.
    runs.ended(holder, 0, 3);
    assert_eq!(
        runs.reserve(waiter, 12 << 20),
        Reservation::Starved(Vec::new())
    );
    let narrow = NodeId(1);
    assert_eq!(other.ask(narrow, 2), vec![narrow]);
    assert_eq!(other.reserve(narrow, CEILING), Reservation::Granted);

    let woken = runs.ended(wide, 0, 3);
    assert!(woken.contains(&waiter), "{woken:?}");
    assert_eq!(runs.reserve(waiter, 12 << 20), Reservation::Granted);
    assert_eq!(pool.room().whole, 0);
    runs.ended(waiter, 0, 3);
    other.ended(narrow, 0, 2);
    assert_eq!(pool.room().reserved, 0);
    assert_eq!(pool.room().above, 0);
}

// After a restore the inner stream, whose volume is a member the outer run
// writes, may reserve before the outer run does, since its shelf already
// holds bytes; it then waits on the outer run's bytes. Whichever of the two
// is above the ceiling, the other is granted beside it, so the outer run
// reads and both end.
#[test]
fn an_inner_run_reserved_first_never_holds_back_the_outer_run_it_waits_on() {
    for (inner_window, outer_window) in [(CEILING, 30 << 20), (30 << 20, CEILING)] {
        let pool = profile_pool();
        let mut runs = Runs::new(1, 2, RunMail::default().sender(), Arc::clone(&pool))
            .on(over_limit(crate::runtime::OverLimit::ScratchThenRam));
        let (outer, inner) = (NodeId(1), NodeId(2));
        assert_eq!(runs.ask(inner, 2), vec![inner]);
        assert_eq!(runs.reserve(inner, inner_window), Reservation::Granted);
        assert_eq!(runs.ask(outer, 2), vec![outer]);
        assert_eq!(runs.reserve(outer, outer_window), Reservation::Granted);
        runs.ended(outer, 0, 2);
        runs.ended(inner, 0, 2);
        assert_eq!(pool.room().reserved, 0);
        assert_eq!(pool.room().above, 0);
        assert_eq!(pool.room().whole, 0);
    }
}

// A run whose decoder borrows its window is lent the window over the
// ceiling from a scratch file, and reserves what a run inside its grant
// reserves: the lent window is not in RAM. A scratch file refused for any reason puts the
// window in RAM.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_lending_run_is_lent_a_scratch_window_and_falls_back_to_ram_when_refused() {
    use crate::pipeline::graph::window::Windows;
    for (ceiling, scratch) in [(WIDE, true), (CEILING, false)] {
        let dir = tempfile::tempdir().unwrap();
        let windows = Arc::new(Windows::new(
            dir.path().to_path_buf(),
            CapacityReader::unknown(),
            holds(u64::MAX),
            ceiling,
        ));
        let pool = profile_pool();
        let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool))
            .on(over_limit(crate::runtime::OverLimit::ScratchThenRam))
            .lending(Arc::clone(&windows));
        let node = NodeId(1);
        assert_eq!(runs.ask(node, 1), vec![node]);
        assert_eq!(
            runs.reserve_for(node, WIDE, Some(Lent(Some(WIDE)))),
            Reservation::Granted
        );
        if scratch {
            assert_eq!(runs.reserved(node), CEILING + RUN_OVERHEAD);
            assert_eq!(windows.live(), (1, WIDE));
        } else {
            assert_eq!(runs.reserved(node), WIDE + RUN_OVERHEAD);
            assert_eq!(windows.live(), (0, 0));
        }
    }
}

// Where no window can be mapped, a lending run over its grant is placed in
// RAM above the ceiling, reserved whole, and no scratch file is made.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[test]
fn where_no_window_can_be_mapped_a_lending_run_is_placed_in_ram_above_the_ceiling() {
    use crate::pipeline::graph::window::Windows;
    let dir = tempfile::tempdir().unwrap();
    let windows = Arc::new(Windows::new(
        dir.path().to_path_buf(),
        CapacityReader::unknown(),
        holds(u64::MAX),
        WIDE,
    ));
    let pool = profile_pool();
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam))
        .lending(Arc::clone(&windows));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(
        runs.reserve_for(node, WIDE, Some(Lent(Some(WIDE)))),
        Reservation::Granted
    );
    assert_eq!(runs.reserved(node), WIDE + RUN_OVERHEAD);
    assert_eq!(pool.room().above, WIDE + RUN_OVERHEAD);
    assert_eq!(windows.live(), (0, 0));
    assert!(!dir.path().join(weaver_graph::names::INTERNAL_DIR).exists());
    runs.ended(node, 0, 1);
    assert_eq!(pool.room().above, 0);
}

// What a run's decoder found in its budget: whether it was handed a
// lender, and the window it was lent for `asked` bytes, with the scratch
// windows the job held while it held it.
struct Borrows {
    asked: usize,
    windows: Arc<crate::pipeline::graph::window::Windows>,
    seen: std::sync::mpsc::Sender<Option<(usize, (u64, u64))>>,
}

impl Extraction for Borrows {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, _: &mut Sink<'_>, budget: weaver_graph::Budget) {
        let seen = budget.lender.map(|lender| {
            let window = lender.lend(self.asked).unwrap();
            (window.len(), self.windows.live())
        });
        self.seen.send(seen).unwrap();
    }
}

// A decoder that borrows declares the window it asks for first. Over the
// window the grant holds one decoder to, that window is lent from scratch
// before the run starts and the decoder's ask takes it: no second file is
// made, and the run reserves what it may keep on its own heap. Within it, the window is
// the decoder's own in RAM, reserved as declared, and the decoder is still
// handed the lender for a later window over its grant.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn a_lending_run_takes_the_window_lent_before_it_started() {
    use crate::pipeline::graph::window::Windows;
    for (asked, scratch) in [(WIDE, true), (CEILING / 2, false)] {
        let dir = tempfile::tempdir().unwrap();
        let mut envelopes = Envelopes::new(dir.path().join("envelopes"));
        let windows = Arc::new(Windows::new(
            dir.path().to_path_buf(),
            CapacityReader::unknown(),
            holds(u64::MAX),
            64 << 20,
        ));
        let pool = profile_pool();
        let mut mail = RunMail::default();
        let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
            .on(over_limit(crate::runtime::OverLimit::ScratchThenRam))
            .lending(Arc::clone(&windows));
        let node = NodeId(1);
        assert_eq!(runs.ask(node, 1), vec![node]);
        let declared = asked.min(CEILING);
        assert_eq!(
            runs.reserve_for(node, declared, Some(Lent(Some(asked)))),
            Reservation::Granted
        );
        let (seen, saw) = std::sync::mpsc::channel();
        let borrows = Borrows {
            asked: asked as usize,
            windows: Arc::clone(&windows),
            seen,
        };
        runs.start(node, Box::new(borrows), envelopes.shelf(node));
        let said = mail.recv().await.unwrap();
        let (len, live) = saw.recv().unwrap().expect("handed the lender");
        assert_eq!(len, asked as usize);
        assert_eq!(live, (1, asked), "one window, the one asked for");
        // Placed on scratch or not, the run reserves what it may hold on
        // its own heap, as declared to the grant: the lent window is not in
        // RAM.
        assert_eq!(runs.reserved(node), declared + RUN_OVERHEAD);
        assert_eq!(live.0, 1, "{scratch}");
        runs.ended(node, said.generation, 1);
        assert_eq!(windows.live(), (0, 0), "every window given back");
        assert_eq!(pool.room().reserved, 0);
    }
}

// Under the efficient policy a decoder that borrows is handed no lender
// and lent nothing: it is held to the ceiling, as one that does not borrow
// is, and refuses a wider window with the sizes.
#[tokio::test]
async fn a_lending_run_under_the_fail_policy_is_handed_no_lender() {
    use crate::pipeline::graph::window::Windows;
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().join("envelopes"));
    let windows = Arc::new(Windows::new(
        dir.path().to_path_buf(),
        CapacityReader::unknown(),
        holds(u64::MAX),
        64 << 20,
    ));
    let pool = profile_pool();
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::Fail))
        .lending(Arc::clone(&windows));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(
        runs.reserve_for(node, WIDE, Some(Lent(Some(WIDE)))),
        Reservation::Granted
    );
    assert_eq!(runs.reserved(node), CEILING + RUN_OVERHEAD);
    assert_eq!(windows.live(), (0, 0));
    let (seen, saw) = std::sync::mpsc::channel();
    let borrows = Borrows {
        asked: WIDE as usize,
        windows: Arc::clone(&windows),
        seen,
    };
    runs.start(node, Box::new(borrows), envelopes.shelf(node));
    let said = mail.recv().await.unwrap();
    assert_eq!(saw.recv().unwrap(), None, "no lender");
    assert!(!runs.widen(node, Some(WIDE), None), "the refusal stands");
    runs.ended(node, said.generation, 1);
    assert_eq!(pool.room().reserved, 0);
}

// What a run's decoder found in its budget, and what its lender did when
// it asked for `asked` bytes past the window lent for it first.
#[derive(Debug, PartialEq, Eq)]
struct Found {
    window_bytes: Option<u64>,
    window_reserved: bool,
    lent: Option<Result<usize, std::io::ErrorKind>>,
}

struct Asks {
    asked: usize,
    seen: std::sync::mpsc::Sender<Found>,
}

impl Extraction for Asks {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, _: &mut Sink<'_>, budget: weaver_graph::Budget) {
        let lent = budget.lender.as_ref().map(|lender| {
            lender
                .lend(self.asked)
                .map(|window| window.len())
                .map_err(|error| error.kind())
        });
        let found = Found {
            window_bytes: budget.window_bytes,
            window_reserved: budget.window_reserved,
            lent,
        };
        self.seen.send(found).unwrap();
    }
}

// Scratch on a disk with room for `room` bytes over a one-byte reserve.
fn scratch_with_room(
    dir: &std::path::Path,
    room: u64,
) -> Arc<crate::pipeline::graph::window::Windows> {
    use crate::operations::{Capacity, CapacityReading};
    let sampled_at = std::time::Instant::now();
    let free = CapacityReader::from_fn(move || {
        Capacity::Known(CapacityReading {
            available_bytes: room + 1,
            total_bytes: room + 1,
            sampled_at,
            stale: false,
        })
    });
    let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 1,
    }));
    Arc::new(crate::pipeline::graph::window::Windows::new(
        dir.to_path_buf(),
        free,
        accountant,
        64 << 20,
    ))
}

// A run placed on scratch whose decoder asks for a window, no wider than
// the one it was placed with, that the disk then cannot hold, comes back
// too large for it: the scratch refused it, not its grant. It starts again
// with the window it was placed with in RAM, above the ceiling, and makes no
// second scratch file; refused again in RAM, its refusal stands.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn a_run_the_scratch_refuses_during_decode_starts_again_in_ram() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().join("envelopes"));
    // Room for the window lent before the run, and nothing more.
    let windows = scratch_with_room(dir.path(), WIDE);
    let pool = profile_pool();
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::ScratchThenRam))
        .lending(Arc::clone(&windows));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(
        runs.reserve_for(node, CEILING, Some(Lent(Some(WIDE)))),
        Reservation::Granted
    );
    assert_eq!(windows.live(), (1, WIDE));
    let (seen, saw) = std::sync::mpsc::channel();
    let asked = (WIDE / 2) as usize;
    runs.start(
        node,
        Box::new(Asks {
            asked,
            seen: seen.clone(),
        }),
        envelopes.shelf(node),
    );
    let said = mail.recv().await.unwrap();
    let found = saw.recv().unwrap();
    assert_eq!(found.lent, Some(Err(std::io::ErrorKind::StorageFull)));
    // As the job widens on the refusal the decoder states.
    assert!(runs.widen(node, Some(asked as u64), None));
    runs.ended(node, said.generation, 1);
    assert_eq!(windows.live(), (0, 0));

    assert_eq!(
        runs.reserve_for(node, CEILING, Some(Lent(Some(WIDE)))),
        Reservation::Granted
    );
    assert_eq!(windows.live(), (0, 0), "no second scratch file");
    assert_eq!(runs.reserved(node), WIDE + RUN_OVERHEAD);
    runs.start(node, Box::new(Asks { asked, seen }), envelopes.shelf(node));
    let said = mail.recv().await.unwrap();
    let found = saw.recv().unwrap();
    assert_eq!(found.window_bytes, Some(WIDE));
    assert!(found.window_reserved);
    assert!(
        !runs.widen(node, Some(asked as u64), None),
        "refused again in RAM, it stands"
    );
    runs.ended(node, said.generation, 1);
    assert_eq!(pool.room().reserved, 0);
}

// Under the efficient policy no run is placed on scratch, so a refusal at
// or under its window stands as it always did.
#[tokio::test]
async fn under_the_fail_policy_a_refusal_within_the_window_stands() {
    let dir = tempfile::tempdir().unwrap();
    let mut envelopes = Envelopes::new(dir.path().join("envelopes"));
    let windows = scratch_with_room(dir.path(), WIDE);
    let pool = profile_pool();
    let mut mail = RunMail::default();
    let mut runs = Runs::new(1, 1, mail.sender(), Arc::clone(&pool))
        .on(over_limit(crate::runtime::OverLimit::Fail))
        .lending(Arc::clone(&windows));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(
        runs.reserve_for(node, CEILING, Some(Lent(Some(WIDE)))),
        Reservation::Granted
    );
    let (seen, saw) = std::sync::mpsc::channel();
    runs.start(
        node,
        Box::new(Asks {
            asked: CEILING as usize,
            seen,
        }),
        envelopes.shelf(node),
    );
    let said = mail.recv().await.unwrap();
    assert_eq!(saw.recv().unwrap().lent, None, "no lender");
    assert!(!runs.widen(node, Some(CEILING), None));
    assert!(!runs.rerunning(node));
    runs.ended(node, said.generation, 1);
    assert_eq!(windows.live(), (0, 0));
    assert_eq!(pool.room().reserved, 0);
}

// A run placed on scratch reserves from the pool what a run inside its
// grant reserves: the window its object declares, to the ceiling, which
// covers what its decoder may keep on its own heap and the workers a grant
// whose memory is wider than its window admits side by side. The lent
// window itself is scratch and not counted.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_run_placed_on_scratch_reserves_what_a_run_in_its_grant_reserves() {
    use crate::pipeline::graph::window::Windows;
    let memory = 2 * CEILING;
    let hardware = crate::pipeline::graph::hardware::Hardware::granting(
        crate::pipeline::graph::hardware::Grant {
            run: weaver_graph::Budget {
                memory_bytes: Some(memory),
                window_bytes: Some(CEILING),
                threads: 1,
                ..weaver_graph::Budget::default()
            },
            over_limit: crate::runtime::OverLimit::ScratchThenRam,
            machine_bytes: Some(64 << 20),
            ..Default::default()
        },
    );
    for (declared, lent) in [(WIDE, WIDE), (CEILING / 2, WIDE)] {
        let dir = tempfile::tempdir().unwrap();
        let windows = Arc::new(Windows::new(
            dir.path().to_path_buf(),
            CapacityReader::unknown(),
            holds(u64::MAX),
            64 << 20,
        ));
        let pool = bounded(memory + RUN_OVERHEAD);
        let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&pool))
            .on(hardware.clone())
            .lending(Arc::clone(&windows));
        let node = NodeId(1);
        assert_eq!(runs.ask(node, 1), vec![node]);
        assert_eq!(
            runs.reserve_for(node, declared, Some(Lent(Some(lent)))),
            Reservation::Granted
        );
        assert_eq!(windows.live(), (1, lent), "placed on scratch");
        let in_grant = declared.min(memory) + RUN_OVERHEAD;
        assert_eq!(runs.reserved(node), in_grant, "{declared}");
        assert_eq!(pool.room().reserved, in_grant);
    }
}
