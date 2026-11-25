// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A mixed 7z store lent out while its run stands. The store places its
// stored members itself and its run makes the coded ones; what the run makes
// is recorded as it starts, so a refusal the run says, a repair that retracts
// the run, or a start that falls while the store is out never reaches a
// stored member, and a start that falls while it is out happens once it is
// back.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;

use sevenz_turbo::encoder_options::{AesEncoderOptions, DeltaOptions, Lzma2Options};
use sevenz_turbo::{
    ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password, SourceReader,
};
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{MemberId, NodeId};

use super::GraphJob;
use super::loan_tests::holds;
use crate::pipeline::graph::engine::{DecodePool, Heard, RunMail, Runs};
use crate::pipeline::graph::hardware::Hardware;
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::tests::{decoded, file};
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::tests::engines::harness::payload;
use crate::pipeline::tests::engines::loans::FRAME;
use weaver_server_disk::operations::disk::CapacityReader;

type Loan = super::works::HeldLoan;

const SECRET: &str = "lumen-quill-secret";
const LIMIT: usize = 1;
// The store numbers members as the archive does: the coded folder holds
// the second and third.
const STORED: [u32; 2] = [0, 3];
const CODED: [u32; 2] = [1, 2];

type Files = Vec<(&'static str, Vec<u8>)>;

// The end header enciphered, the stored folders under AES, the coded
// folder plain. Every stored size is a whole number of AES blocks, so the
// coded pack sits right after the first stored member.
fn archive(coded: Vec<EncoderConfiguration>) -> (Vec<u8>, Files) {
    let stored: Vec<EncoderConfiguration> = vec![
        AesEncoderOptions::new(Password::new(SECRET)).into(),
        EncoderConfiguration::new(EncoderMethod::COPY),
    ];
    archive_under(true, stored, coded)
}

// The header and the stored folders plain, the coded folder under AES over
// LZMA2: no candidate is tried before the run, which asks for one itself.
fn asking() -> (Vec<u8>, Files) {
    let mut coded: Vec<EncoderConfiguration> =
        vec![AesEncoderOptions::new(Password::new(SECRET)).into()];
    coded.extend(lzma2());
    archive_under(
        false,
        vec![EncoderConfiguration::new(EncoderMethod::COPY)],
        coded,
    )
}

fn archive_under(
    header: bool,
    stored: Vec<EncoderConfiguration>,
    coded: Vec<EncoderConfiguration>,
) -> (Vec<u8>, Files) {
    let files: Files = vec![
        ("ember.mkv", payload(5 * FRAME, 41)),
        ("quill.mkv", payload(3 * FRAME - 3, 42)),
        ("cinder.nfo", payload(2 * FRAME + 5, 43)),
        ("lumen.mkv", payload(4 * FRAME, 44)),
    ];
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_encrypt_header(header);
    writer.set_content_methods(stored.clone());
    writer
        .push_archive_entry(
            ArchiveEntry::new_file(files[0].0),
            Some(Cursor::new(files[0].1.clone())),
        )
        .unwrap();
    writer.set_content_methods(coded);
    writer
        .push_archive_entries(
            vec![
                ArchiveEntry::new_file(files[1].0),
                ArchiveEntry::new_file(files[2].0),
            ],
            vec![
                SourceReader::new(Cursor::new(files[1].1.clone())),
                SourceReader::new(Cursor::new(files[2].1.clone())),
            ],
        )
        .unwrap();
    writer.set_content_methods(stored);
    writer
        .push_archive_entry(
            ArchiveEntry::new_file(files[3].0),
            Some(Cursor::new(files[3].1.clone())),
        )
        .unwrap();
    (writer.finish().unwrap().into_inner(), files)
}

struct Rig {
    job: GraphJob,
    mail: RunMail,
    slots: RepairSlots,
    volume: Vec<u8>,
    files: Files,
    in_hand: Vec<Loan>,
    // Each generation a run emitted under, with the members it named.
    spoken: Vec<(u64, String)>,
    directory: tempfile::TempDir,
}

impl Rig {
    fn new(archive: (Vec<u8>, Files)) -> Self {
        Self::offering(archive, &[SECRET])
    }

    fn offering((volume, files): (Vec<u8>, Files), candidates: &[&str]) -> Self {
        let sizes: Vec<u32> = volume
            .chunks(FRAME)
            .map(|chunk| chunk.len() as u32)
            .collect();
        let directory = tempfile::tempdir().unwrap();
        let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
            resident_limit: u64::MAX,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        }));
        let writer = NamespaceWriter::new(
            1,
            directory.path().to_owned(),
            accountant,
            CapacityReader::unknown(),
        );
        let mail = RunMail::default();
        let runs = Runs::new(1, 1, mail.sender(), DecodePool::unbounded());
        let mut job = GraphJob::new(
            &[file("lantern.7z", &sizes)],
            Box::new(Hardware::default()),
            writer,
            runs,
            u32::MAX,
        )
        .offering(
            candidates
                .iter()
                .map(|&candidate| candidate.to_owned())
                .collect(),
        );
        job.hold_loans();
        Self {
            job,
            mail,
            slots: RepairSlots::default(),
            volume,
            files,
            in_hand: Vec::new(),
            spoken: Vec::new(),
            directory,
        }
    }

    // Every article in order, each loan kept in hand.
    fn serve(&mut self, limit: usize) {
        let length = self.volume.len() as u64;
        for ordinal in 0..self.volume.len().div_ceil(FRAME) {
            let start = ordinal * FRAME;
            let end = (start + FRAME).min(self.volume.len());
            let bytes = self.volume[start..end].to_vec();
            self.job
                .intake(
                    decoded(0, ordinal as u32, start as u64, &bytes, Some(length)),
                    &mut self.slots,
                    limit,
                )
                .unwrap();
            self.take();
        }
    }

    fn take(&mut self) {
        self.in_hand.extend(self.job.held_loans());
    }

    // Every loan in hand comes back under `limit`, and every one their
    // return lends, until none is out.
    fn give_back(&mut self, limit: usize) {
        while !self.in_hand.is_empty() {
            for loan in std::mem::take(&mut self.in_hand) {
                let said = self.job.worked_loan(loan);
                self.job.run_said(said, &mut self.slots, limit).unwrap();
                self.take();
            }
        }
    }

    // The node a run was asked for.
    fn run_node(&self) -> NodeId {
        (1..64)
            .map(NodeId)
            .find(|node| self.job.runs.has(*node))
            .expect("a run was asked")
    }

    // What the node's run says, heard by the job, until it returns.
    async fn hear_until_ended(&mut self, node: NodeId) {
        loop {
            let said = self.mail.recv().await.expect("the run speaks");
            let ended = said.node == node && matches!(said.heard, Heard::Ended);
            if said.node == node && matches!(said.heard, Heard::Emit(_) | Heard::Paid(..)) {
                self.spoken
                    .push((said.generation, format!("{:?}", said.heard)));
            }
            self.job.run_said(said, &mut self.slots, LIMIT).unwrap();
            self.take();
            if ended {
                return;
            }
        }
    }

    // Every loan back, then whatever is due lent and given back, until the
    // job holds nothing out.
    fn settle(&mut self) {
        loop {
            self.give_back(LIMIT);
            self.job.lend_work(LIMIT).unwrap();
            self.take();
            if self.in_hand.is_empty() {
                return;
            }
        }
    }

    fn whole(&self, index: u32) -> bool {
        holds(self.directory.path(), &self.files[index as usize].1)
    }

    // The stored members stand in the store's namespace, unrefused, and are
    // on disk whole.
    fn assert_stored_stand(&self, node: NodeId) {
        let namespace = self
            .job
            .tree
            .namespace(node)
            .expect("the store's namespace");
        for index in STORED {
            assert!(
                namespace.get(MemberId(index)).is_some(),
                "stored member {index} was retracted"
            );
            assert_eq!(
                self.job.writer.refused(node, MemberId(index)),
                None,
                "stored member {index} refused"
            );
            assert!(self.whole(index), "stored member {index} whole on disk");
        }
    }

    // Every member a run named, and the generations it emitted under.
    fn named(&self) -> (BTreeSet<u32>, BTreeSet<u64>) {
        let mut members = BTreeSet::new();
        let mut generations = BTreeSet::new();
        for (generation, message) in &self.spoken {
            generations.insert(*generation);
            let mut rest = message.as_str();
            while let Some(at) = rest.find("MemberId(") {
                rest = &rest[at + "MemberId(".len()..];
                let end = rest.find(')').unwrap();
                members.insert(rest[..end].parse().unwrap());
            }
        }
        (members, generations)
    }
}

fn lzma2() -> Vec<EncoderConfiguration> {
    vec![Lzma2Options::from_level(1).into()]
}

// A filter that codes each byte alone: a byte wrong in its pack is a byte
// wrong in what it decodes, which the folder's CRC finds.
fn delta() -> Vec<EncoderConfiguration> {
    vec![DeltaOptions::from_distance(1).into()]
}

// Every article served and every loan given back: the header is
// deciphered, the stored members streamed, and the run started.
fn started(archive: (Vec<u8>, Files)) -> (Rig, NodeId) {
    let mut rig = Rig::new(archive);
    rig.serve(LIMIT);
    rig.give_back(LIMIT);
    let node = rig.run_node();
    assert_eq!(rig.job.runs.holding(), vec![node], "the run started");
    (rig, node)
}

// (i) A byte of the coded pack wrong in the post: the run reads it, and its
// refusal arrives while the store is out. It lands on the coded members the
// run's record names, never on a stored one.
#[tokio::test]
async fn a_hybrid_runs_refusal_heard_while_its_store_is_lent_refuses_only_the_coded_members() {
    let (mut volume, files) = archive(delta());
    volume[32 + files[0].1.len() + 7] ^= 0x5a;
    let (mut rig, node) = started((volume, files));
    let store = rig.job.lend_object_for_test(node).expect("the store lends");
    rig.hear_until_ended(node).await;
    assert!(rig.job.tree.lent(node), "the store was out throughout");
    for index in CODED {
        assert!(
            rig.job.writer.refused(node, MemberId(index)).is_some(),
            "coded member {index} carries the run's refusal"
        );
    }
    rig.assert_stored_stand(node);
    rig.job.take_back_for_test(node, store, LIMIT).unwrap();
    rig.settle();
    rig.assert_stored_stand(node);
    for index in CODED {
        assert!(!rig.whole(index), "coded member {index} cannot be whole");
    }
    let (members, generations) = rig.named();
    assert!(members.is_subset(&BTreeSet::from(CODED)), "{members:?}");
    assert_eq!(generations.len(), 1, "the refused run is not asked again");
}

// (ii) The run ended, then a repair restarts its shelf while the store is
// out: nothing is retracted until the store is back, and then only the
// coded members, which a run made after the return makes again.
#[tokio::test]
async fn a_hybrid_run_restarted_while_its_store_is_lent_retracts_only_the_coded_members_after() {
    let (mut rig, node) = started(archive(lzma2()));
    rig.hear_until_ended(node).await;
    rig.settle();
    for index in 0..4 {
        assert!(rig.whole(index), "member {index} whole");
    }
    let first = rig.named().1;
    assert_eq!(first.len(), 1, "{first:?}");
    let store = rig.job.lend_object_for_test(node).expect("the store lends");
    rig.job.writer.restart_shelf(node);
    rig.job.resume_runs(&mut rig.slots, LIMIT).unwrap();
    rig.take();
    assert!(
        rig.job.runs.holding().is_empty(),
        "nothing started while out"
    );
    rig.assert_stored_stand(node);
    for index in CODED {
        assert!(
            rig.job
                .tree
                .namespace(node)
                .and_then(|namespace| namespace.get(MemberId(index)))
                .is_some(),
            "coded member {index} retracted while the store was out"
        );
    }
    rig.job.take_back_for_test(node, store, LIMIT).unwrap();
    rig.take();
    assert_eq!(rig.job.runs.holding(), vec![node], "the run runs again");
    rig.hear_until_ended(node).await;
    rig.settle();
    rig.assert_stored_stand(node);
    for index in 0..4 {
        assert!(rig.whole(index), "member {index} whole");
    }
    let (members, generations) = rig.named();
    assert_eq!(members, BTreeSet::from(CODED), "the members runs named");
    assert_eq!(generations.len(), 2, "{generations:?}");
}

// (iii) Every article served with no slot, as a paused job is: the run is
// asked and waits. Then the store lent: the slot
// that opens while the store is out starts nothing and holds nothing. Once
// the store is back the run starts, once.
#[tokio::test]
async fn a_hybrid_start_that_falls_while_its_store_is_lent_happens_once_after_the_return() {
    let mut rig = Rig::new(archive(lzma2()));
    rig.serve(0);
    rig.give_back(0);
    let node = rig.run_node();
    assert!(rig.job.runs.holding().is_empty(), "no slot, no start");
    let store = rig.job.lend_object_for_test(node).expect("the store lends");
    rig.job.resume_runs(&mut rig.slots, LIMIT).unwrap();
    rig.take();
    assert!(
        !rig.job.runs.has(node),
        "the start deferred, no run left queued"
    );
    assert!(rig.job.runs.holding().is_empty(), "no slot held while out");
    rig.job.take_back_for_test(node, store, LIMIT).unwrap();
    rig.take();
    assert_eq!(rig.job.runs.holding(), vec![node], "started on the return");
    rig.hear_until_ended(node).await;
    rig.settle();
    for index in 0..4 {
        assert!(rig.whole(index), "member {index} whole");
    }
    let (members, generations) = rig.named();
    assert_eq!(members, BTreeSet::from(CODED), "the members runs named");
    assert_eq!(generations.len(), 1, "one start: {generations:?}");
}

// (iv) The coded folder's run asks for a password while its store is out,
// each time: the offer is queued for the store, and the run's record, not
// the store's face, says the run starts over under it, once the store is
// back. Nothing starts while it is out. Every wrong candidate is asked past
// so, and the right one, wherever it stands, makes every coded member.
async fn asked_while_lent_through(candidates: &[&str]) {
    let mut rig = Rig::offering(asking(), candidates);
    rig.serve(LIMIT);
    rig.give_back(LIMIT);
    let node = rig.run_node();
    let mut lent = 0;
    while !rig.job.runs.holding().is_empty() {
        let store = rig.job.lend_object_for_test(node).expect("the store lends");
        lent += 1;
        rig.hear_until_ended(node).await;
        assert!(rig.job.tree.lent(node), "the store was out throughout");
        assert!(
            rig.job.runs.holding().is_empty(),
            "nothing started while out"
        );
        rig.job.take_back_for_test(node, store, LIMIT).unwrap();
        rig.take();
        rig.settle();
    }
    // The first run has no candidate and asks at once; then one run under
    // each candidate.
    assert_eq!(lent, candidates.len() + 1, "one run per candidate");
    for index in 0..4 {
        assert!(rig.whole(index), "member {index} whole");
    }
    rig.assert_stored_stand(node);
    for index in CODED {
        assert_eq!(rig.job.writer.refused(node, MemberId(index)), None);
    }
    let (members, generations) = rig.named();
    assert_eq!(members, BTreeSet::from(CODED), "the members runs named");
    assert_eq!(generations.len(), lent, "every run spoke: {generations:?}");
    // A run under a wrong candidate names its first member and retracts it
    // when the folder fails to decode; only the last, under the right
    // candidate, gives a member's bytes.
    let naming: BTreeSet<u64> = rig
        .spoken
        .iter()
        .filter(|(_, message)| message.starts_with("Emit(Bytes"))
        .map(|(generation, _)| *generation)
        .collect();
    assert_eq!(naming.len(), 1, "{naming:?}");
    assert_eq!(naming.last(), generations.last(), "{naming:?}");
}

#[tokio::test]
async fn a_hybrid_run_asking_while_its_store_is_lent_runs_again_under_the_right_candidate() {
    asked_while_lent_through(&["lumen-wrong-guess", SECRET]).await;
}

#[tokio::test]
async fn a_hybrid_run_asking_while_its_store_is_lent_reaches_the_right_candidate_last_of_four() {
    asked_while_lent_through(&[
        "lumen-wrong-one",
        "lumen-wrong-two",
        "lumen-wrong-three",
        SECRET,
    ])
    .await;
}
