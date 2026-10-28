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
use super::{Heard, RUN_OVERHEAD, Reservation, RunMail, Runs};
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::envelope::{Envelopes, Shelf};
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
// the read ends short where they begin, and the next fails on exactly them,
// until the run says it read on past them.
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
    volumes.bridged(lost);
    assert!(!volumes.lost(), "a span the run read on past still ends it");
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
fn forgetting_the_node_stops_a_waiting_read() {
    let dir = tempfile::tempdir().unwrap();
    let (mut envelopes, shelf) = shelved(dir.path());
    envelopes.keep(NODE, 0, 0, b"0", Provenance::Fresh).unwrap();
    let volumes = reader(&shelf);
    let read = std::thread::spawn(move || read(&volumes, 0, 4));
    envelopes.forget(NODE).unwrap();
    let error = read.join().unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

// A run that says nothing and returns.
struct Quiet;

impl Extraction for Quiet {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, _: &mut Sink<'_>, _: weaver_graph::Budget) {}
}

fn unbounded() -> Arc<HoldsAccountant> {
    bounded(u64::MAX)
}

fn bounded(resident_limit: u64) -> Arc<HoldsAccountant> {
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
// both on the run and against the accountant. Asking again with a wider
// window, as a restart would, takes nothing more; the end gives back all
// of it.
#[test]
fn memory_reserved_is_the_declared_window_plus_the_fixed_overhead_and_never_grows() {
    const WINDOW: u64 = 3 << 20;
    let accountant = unbounded();
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), Arc::clone(&accountant));
    let node = NodeId(5);
    let namespace = HoldsNamespace { scope: 1, node };
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, WINDOW), Reservation::Granted);
    assert_eq!(runs.reserved(node), WINDOW + RUN_OVERHEAD);
    assert_eq!(
        accountant.namespace_usage(namespace).resident,
        WINDOW + RUN_OVERHEAD
    );

    assert_eq!(runs.reserve(node, 4 * WINDOW), Reservation::Granted);
    assert_eq!(runs.reserved(node), WINDOW + RUN_OVERHEAD, "it never grows");
    assert_eq!(accountant.usage().resident, WINDOW + RUN_OVERHEAD);

    runs.ended(node, 0, 1);
    assert_eq!(runs.reserved(node), 0);
    assert_eq!(accountant.usage().resident, 0);
    assert_eq!(accountant.usage().namespaces, 0);
}

// A reservation that does not fit beside what is resident gives the slot
// to the next waiter and waits; once resident bytes are released it asks
// again, through the same release that frees a slot.
#[test]
fn a_window_that_does_not_fit_yields_its_slot_and_asks_again_once_memory_is_released() {
    const WINDOW: u64 = 1 << 20;
    let accountant = bounded(2 * (WINDOW + RUN_OVERHEAD));
    let mut runs = Runs::new(1, 2, RunMail::default().sender(), Arc::clone(&accountant));
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

// A reservation larger than the resident limit itself can never fit.
#[test]
fn a_window_larger_than_the_resident_limit_is_refused() {
    let mut runs = Runs::new(1, 1, RunMail::default().sender(), bounded(RUN_OVERHEAD));
    let node = NodeId(1);
    assert_eq!(runs.ask(node, 1), vec![node]);
    assert_eq!(runs.reserve(node, 1), Reservation::Refused);
    assert_eq!(runs.reserved(node), 0);
}

// A graph job of echo volumes, one article each, read by echo engines.
struct Echoing {
    job: GraphJob,
    mail: RunMail,
    bids: Arc<Mutex<Vec<Seek>>>,
    volumes: Vec<Vec<u8>>,
    dir: tempfile::TempDir,
    accountant: Arc<HoldsAccountant>,
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

// Echo volumes whose engine declares `window`, reserved against `accountant`.
fn echoing_within(count: u32, window: u64, accountant: Arc<HoldsAccountant>) -> Echoing {
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
    let echoes = Echoes {
        volumes: count,
        bids: Arc::default(),
        window,
        gate: None,
    };
    let bids = Arc::clone(&echoes.bids);
    let dir = tempfile::tempdir().unwrap();
    let writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let mail = RunMail::default();
    let runs = Runs::new(1, 1, mail.sender(), Arc::clone(&accountant));
    let job = GraphJob::new(&files, Box::new(echoes), writer, runs, u32::MAX);
    Echoing {
        job,
        mail,
        bids,
        volumes,
        dir,
        accountant,
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
        self.job.seek_round(|_| true).unwrap();
        std::mem::take(&mut *self.bids.lock().unwrap())
    }

    fn echoed(&self, member: u32) -> Vec<u8> {
        std::fs::read(self.dir.path().join(format!("echo-{member}.bin"))).unwrap()
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
    assert_eq!(
        echoing
            .accountant
            .namespace_usage(HoldsNamespace {
                scope: 1,
                node: engine
            })
            .resident,
        WINDOW + RUN_OVERHEAD
    );
    while !echoing.hear().await {}
    assert_eq!(echoing.echoed(0), body(0));
    assert_eq!(echoing.accountant.usage().resident, 0);
}

// An engine whose window can never fit is refused to the tree, and its
// volume is a leaf that says why.
#[tokio::test]
async fn an_engine_whose_window_can_never_fit_is_refused_unsupported() {
    let mut echoing = echoing_within(1, 1 << 30, bounded(64 << 20));
    echoing.deliver(0);
    assert!(echoing.job.tree().engines().is_empty());
    assert_eq!(
        echoing.job.writer().refused(NodeId::ROOT, MemberId(0)),
        Some(Why::Unsupported)
    );
    assert!(!echoing.job.runs().live());
    assert_eq!(echoing.accountant.usage().resident, 0);
}

// Another namespace holds most of the resident limit when the engine wants
// to start, so its run waits; the next step after those bytes are released
// starts it.
#[tokio::test]
async fn an_engine_turned_away_for_memory_starts_once_resident_bytes_are_released() {
    const WINDOW: u64 = 1 << 20;
    let accountant = bounded(WINDOW + RUN_OVERHEAD);
    let other = HoldsNamespace {
        scope: 2,
        node: NodeId::ROOT,
    };
    assert_eq!(
        accountant.reserve_resident(other, WINDOW),
        weaver_graph::holds::Reserve::Granted
    );
    let mut echoing = echoing_within(2, WINDOW, Arc::clone(&accountant));
    echoing.deliver(0);
    let engine = echoing.engine();
    assert_eq!(echoing.job.runs().reserved(engine), 0, "it did not fit");
    assert!(echoing.job.runs().holding().is_empty(), "nor holds a slot");
    assert!(echoing.job.runs().live(), "it waits for memory");

    accountant.release_resident(other, WINDOW);
    echoing.deliver(1);
    assert_eq!(echoing.job.runs().reserved(engine), WINDOW + RUN_OVERHEAD);
    while !echoing.hear().await {}
    for member in 0..2 {
        assert_eq!(echoing.echoed(member), body(member));
    }
    assert_eq!(accountant.usage().resident, 0);
}

// A posted volume the run already read closes again at another length, as
// a recovery set's description corrects it: every offset after it moved, so
// the run built on the old ones starts over.
#[tokio::test]
async fn a_volume_the_run_read_closing_at_another_length_restarts_the_run() {
    let mut echoing = echoing(3);
    echoing.deliver(0);
    echoing.until_closed(0).await;
    let engine = echoing.engine();
    let before = echoing.job.runs().generation(engine);
    assert!(before.is_some(), "the run is live");
    let namespace = echoing.job.namespace_mut(NodeId::ROOT).unwrap();
    let name = namespace.get(MemberId(0)).unwrap().name.clone();
    let length = namespace.get(MemberId(0)).unwrap().length.unwrap();
    namespace.recognise(MemberId(0), &name, length - 1);
    echoing.deliver(1);
    assert_ne!(echoing.job.runs().generation(engine), before);
}
