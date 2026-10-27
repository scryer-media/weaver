// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member its object gave up becomes a leaf, or a volume of the engine bound
// in the object's place, with every byte it already received; and a password
// is offered one candidate at a time until one opens or none is left.

mod fakes;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{MemberId, NodeId, VolumeIndex, Why};

use self::fakes::{Fakes, Mode, OPENS};
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::engine::echo::{HEADER, SIGNATURE};
use crate::pipeline::graph::engine::{Heard, RunMail, Runs};
use crate::pipeline::graph::job::{GraphJob, RootFile};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::intake::Decoded;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::status::Delivery;
use crate::pipeline::graph::status::RepairSlots;

const CANDIDATES: [&str; 4] = ["lantern-one", "lantern-two", OPENS, "lantern-four"];
const WRONG: [&str; 4] = ["lantern-one", "lantern-two", "lantern-four", "lantern-five"];

struct Refusing {
    job: GraphJob,
    mail: RunMail,
    volumes: Vec<Vec<u8>>,
    offers: Arc<Mutex<Vec<String>>>,
    starts: Arc<AtomicUsize>,
    dir: tempfile::TempDir,
}

// The body of volume `index`: bytes no format sniffs as its own.
fn body(index: u32) -> Vec<u8> {
    (0..40u8)
        .map(|byte| byte.wrapping_mul(7).wrapping_add(index as u8) | 0x80)
        .collect()
}

fn refusing(mode: Mode, count: u32, candidates: &[&str]) -> Refusing {
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
    let fakes = Fakes {
        mode,
        volumes: count,
        offers: Arc::default(),
        starts: Arc::default(),
    };
    let (offers, starts) = (Arc::clone(&fakes.offers), Arc::clone(&fakes.starts));
    let dir = tempfile::tempdir().unwrap();
    let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }));
    let writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let mail = RunMail::default();
    let runs = Runs::new(1, 1, mail.sender(), accountant);
    let job = GraphJob::new(&files, Box::new(fakes), writer, runs, u32::MAX).offering(
        candidates
            .iter()
            .map(|candidate| candidate.to_string())
            .collect(),
    );
    Refusing {
        job,
        mail,
        volumes,
        offers,
        starts,
        dir,
    }
}

impl Refusing {
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

    fn deliver_all(&mut self) {
        for volume in 0..self.volumes.len() as u32 {
            self.deliver(volume);
        }
    }

    // Hear what the runs say until the current one returned.
    async fn until_ended(&mut self) {
        loop {
            let said = self.mail.recv().await.unwrap();
            let ended = matches!(said.heard, Heard::Ended)
                && self.job.runs().current(said.node, said.generation);
            self.job
                .run_said(said, &mut RepairSlots::default(), 1)
                .unwrap();
            if ended {
                return;
            }
        }
    }

    // Hear what the runs say until the root member is a refused leaf.
    async fn until_refused(&mut self, member: u32) {
        while self.refused(member).is_none() {
            let said = self.mail.recv().await.unwrap();
            self.job
                .run_said(said, &mut RepairSlots::default(), 1)
                .unwrap();
        }
    }

    fn refused(&self, member: u32) -> Option<Why> {
        self.job.writer().refused(NodeId::ROOT, MemberId(member))
    }

    fn file(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.dir.path().join(name)).unwrap()
    }

    fn exists(&self, name: &str) -> bool {
        self.dir.path().join(name).exists()
    }

    // A root member is a leaf: nothing is bound over it.
    fn is_leaf(&self, member: u32) -> bool {
        self.job
            .tree()
            .child_of(NodeId::ROOT, MemberId(member))
            .is_none()
    }

    fn offers(&self) -> Vec<String> {
        self.offers.lock().unwrap().clone()
    }

    // Everything status, health and completion read of the job, as text.
    fn status(&self) -> String {
        format!(
            "{:?} {:?} {:?} {:?} {:?}",
            self.job.phase(),
            self.job.namespace(),
            self.job.recovery_summary(),
            self.job.member_files(),
            (0..self.volumes.len() as u32)
                .map(|member| self.refused(member))
                .collect::<Vec<_>>(),
        )
    }
}

#[test]
fn a_store_that_refuses_hands_back_its_volumes_as_leaves_with_their_bytes() {
    let mut refusing = refusing(Mode::Refuse, 2, &[]);
    refusing.deliver(0);
    assert!(
        refusing.exists("inner-0.bin"),
        "the store streamed its member"
    );
    refusing.deliver(1);
    for member in 0..2 {
        assert!(refusing.is_leaf(member));
        assert_eq!(refusing.refused(member), Some(Why::Unsupported));
        assert_eq!(
            refusing.file(&format!("lantern.part{}.rar", member + 1)),
            refusing.volumes[member as usize]
        );
        // The store closed each member itself before it gave up, so it
        // vouched for it: what an object closed whole stays delivered
        // (the partial-extraction ruling of 2026-10-18).
        assert_eq!(refusing.file(&format!("inner-{member}.bin")), body(member));
    }
    assert_eq!(refusing.job.phase(), RootPhase::Done);
    assert!(refusing.status().contains("Unsupported"));
}

// The store closed each of its members whole before it gave up: buried, it
// keeps them, and the delivery lists them whole. The volumes it handed back
// are delivered as posted, never whole: the store refused them.
#[test]
fn what_a_refused_store_closed_whole_is_delivered_whole() {
    let mut refusing = refusing(Mode::Refuse, 2, &[]);
    refusing.deliver_all();
    let delivery = Delivery::read(
        refusing.job.tree(),
        |_| true,
        |_| false,
        |_, _| false,
        |_, _| None,
        |_, _| false,
    );
    assert_eq!(
        delivery,
        Delivery {
            whole: vec!["inner-0.bin".to_string(), "inner-1.bin".to_string()],
            damaged: vec![
                "lantern.part1.rar".to_string(),
                "lantern.part2.rar".to_string(),
            ],
            files: vec![
                super::delivered("lantern.part1.rar", 0, false),
                super::delivered("inner-0.bin", 1, true),
                super::delivered("inner-1.bin", 1, true),
                super::delivered("lantern.part2.rar", 0, false),
            ],
            ..Delivery::default()
        }
    );
    for member in 0..2 {
        assert!(refusing.is_leaf(member));
        assert_eq!(refusing.file(&format!("inner-{member}.bin")), body(member));
    }
}

#[tokio::test]
async fn a_store_that_needs_an_engine_hands_its_volumes_to_one() {
    let mut refusing = refusing(Mode::NeedsEngine, 2, &[]);
    refusing.deliver_all();
    for member in 0..2 {
        assert_eq!(refusing.refused(member), Some(Why::Compressed));
        assert!(!refusing.exists(&format!("inner-{member}.bin")));
    }
    assert_eq!(
        refusing.job.tree().engines().len(),
        1,
        "one engine over both"
    );
    refusing.until_ended().await;
    // The engine read the volumes back from what the store had placed, its
    // header envelopes and its members' files, and wrote what it found.
    for member in 0..2 {
        assert_eq!(refusing.file(&format!("echo-{member}.bin")), body(member));
    }
    assert_eq!(refusing.job.phase(), RootPhase::Done);
}

#[test]
fn a_store_that_refuses_before_placing_anything_gets_every_held_byte_back() {
    let mut refusing = refusing(Mode::Hold, 2, &[]);
    refusing.deliver(0);
    assert!(
        !refusing.exists("lantern.part1.rar"),
        "the bytes were held, not written"
    );
    refusing.deliver(1);
    for member in 0..2 {
        assert!(refusing.is_leaf(member));
        assert_eq!(refusing.refused(member), Some(Why::Unsupported));
        assert_eq!(
            refusing.file(&format!("lantern.part{}.rar", member + 1)),
            refusing.volumes[member as usize]
        );
    }
    assert_eq!(refusing.job.writer().held_bytes(NodeId::ROOT), 0);
    assert_eq!(refusing.job.phase(), RootPhase::Done);
}

#[test]
fn the_candidates_are_offered_in_order_until_one_opens() {
    let mut refusing = refusing(Mode::Locked, 1, &CANDIDATES);
    refusing.deliver(0);
    assert_eq!(refusing.offers(), CANDIDATES[..3].to_vec());
    assert_eq!(refusing.file("opened.bin"), body(0));
    assert!(!refusing.is_leaf(0));
    assert_eq!(refusing.refused(0), None);
    assert_eq!(refusing.job.phase(), RootPhase::Done);
}

#[test]
fn a_store_no_candidate_opens_refuses_after_the_last() {
    let mut refusing = refusing(Mode::Locked, 1, &WRONG);
    refusing.deliver(0);
    assert_eq!(refusing.offers(), WRONG.to_vec());
    assert!(refusing.is_leaf(0));
    assert_eq!(refusing.refused(0), Some(Why::EncryptedHeader));
    assert_eq!(refusing.file("lantern.part1.rar"), refusing.volumes[0]);
    assert_eq!(refusing.job.phase(), RootPhase::Done);
}

#[tokio::test]
async fn an_engine_given_a_password_restarts_its_run_once() {
    let mut refusing = refusing(Mode::LockedEngine, 1, &[OPENS, "lantern-four"]);
    refusing.deliver(0);
    refusing.until_ended().await;
    assert_eq!(refusing.starts.load(Ordering::SeqCst), 2);
    assert_eq!(refusing.offers(), vec![OPENS.to_string()]);
    assert_eq!(refusing.file("unlocked.bin"), body(0));
    assert_eq!(refusing.job.phase(), RootPhase::Done);
}

#[tokio::test]
async fn an_engine_that_refuses_leaves_a_leaf_nothing_is_bound_over() {
    let mut refusing = refusing(Mode::RefusingEngine, 1, &[]);
    refusing.deliver(0);
    refusing.until_refused(0).await;
    assert_eq!(refusing.refused(0), Some(Why::Unsupported));
    assert!(refusing.is_leaf(0));
    assert!(refusing.job.tree().engines().is_empty());
    assert_eq!(refusing.file("lantern.part1.rar"), refusing.volumes[0]);
    assert_eq!(refusing.starts.load(Ordering::SeqCst), 1);
    assert_eq!(refusing.job.phase(), RootPhase::Done);
}

#[test]
fn no_candidate_reaches_a_log_or_the_status() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(Captured(Arc::clone(&events)));
    let mut opened = refusing(Mode::Locked, 1, &CANDIDATES);
    opened.deliver(0);
    let mut exhausted = refusing(Mode::Locked, 1, &WRONG);
    exhausted.deliver(0);
    tracing::info!(status = %opened.status(), "opened");
    let snapshots = [opened.status(), exhausted.status()];
    let events = events.lock().unwrap();
    assert!(!events.is_empty(), "the capture hears events");
    for candidate in CANDIDATES.iter().chain(&WRONG) {
        for text in events.iter().chain(&snapshots) {
            assert!(!text.contains(candidate), "{candidate} in {text}");
        }
    }
}

// Every event's message and fields, as one line each, at every level.
struct Captured(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for Captured {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(self.0, " {field}={value:?}");
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
