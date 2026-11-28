// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the root still fetches unasked is kept between reads: the job brings
// it up to date on its mutable paths, classifying only the volumes that
// entered or left the volumes to come and the members marked since, and a
// read answers from it. Under the fixtures every such read is held to the
// walk it replaced, so each test here also checks the answer.

use std::sync::Arc;

use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{Namespace, NodeId};

use super::verification_tests::rows;
use super::{Caps, GraphJob, Pending, RootFile, Storage};
use crate::pipeline::graph::checkpoint;
use crate::pipeline::graph::engine::echo::Echoes;
use crate::pipeline::graph::engine::{DecodePool, RunMail, Runs};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::tests::{decoded, file, job_with};
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::tests::engines::harness::{ARTICLE, Post, par2_set, payload};
use weaver_server_disk::operations::disk::CapacityReader;

const SIZE: u32 = 100;

fn intake(job: &mut GraphJob, volume: u32, ordinal: u32, bytes: &[u8], length: u64) {
    let offset = u64::from(ordinal) * bytes.len() as u64;
    job.intake(
        decoded(volume, ordinal, offset, bytes, Some(length)),
        &mut RepairSlots::default(),
        0,
    )
    .unwrap();
}

// Read what is to come and the phase, as the pipeline does after a step,
// and say how many reads walked.
fn read(job: &mut GraphJob) -> u64 {
    job.phase_change();
    let _ = job.coming();
    let _ = job.checked_phase();
    let (_, coming, payload) = job.coming_counts();
    coming + payload
}

// V plain files of two articles each. The first refresh classifies the V
// volumes to come. Each first article marks its member, which is still to
// come, and is classified again: V more. Each second article takes its
// volume out of the volumes to come, one flip, and classifies nothing. So
// S = V steps of one flip each cost 2V classifications, not the V per read
// a walk costs, and no read walks.
#[test]
fn what_is_to_come_is_classified_once_per_change() {
    const V: u32 = 12;
    let files: Vec<RootFile> = (0..V)
        .map(|volume| file(&format!("f{volume}.bin"), &[SIZE, SIZE]))
        .collect();
    let (mut job, _dir, _) = job_with(&files, Vec::new());
    let length = u64::from(2 * SIZE);
    let bytes = vec![0x5a; SIZE as usize];

    assert_eq!(read(&mut job), 0);
    assert_eq!(job.coming_counts().0, u64::from(V));
    assert!(job.coming().payload);
    for volume in 0..V {
        intake(&mut job, volume, 0, &bytes, length);
        assert_eq!(read(&mut job), 0, "volume {volume}'s first article");
        assert_eq!(job.coming_counts().0, u64::from(V + volume + 1));
    }
    for volume in 0..V {
        intake(&mut job, volume, 1, &bytes, length);
        assert_eq!(read(&mut job), 0, "volume {volume}'s second article");
        assert_eq!(job.coming_counts().0, u64::from(2 * V));
        assert_eq!(job.coming().payload, volume + 1 < V);
    }
    assert_eq!(job.coming_counts(), (u64::from(2 * V), 0, 0));
}

// A plain file of two articles, its PAR2 index and the set's one recovery
// volume, posted in that order: the root's volumes 0, 1 and 2.
struct Set {
    job: GraphJob,
    files: Vec<RootFile>,
    par2: Vec<Post>,
    _directory: tempfile::TempDir,
}

const PLAIN: u32 = 0;
const INDEX: u32 = 1;
const RECOVERY: u32 = 2;

fn sizes(post: &Post) -> Vec<u32> {
    post.articles(ARTICLE)
        .map(|bytes| bytes.len() as u32)
        .collect()
}

fn accountant() -> Arc<HoldsAccountant> {
    Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }))
}

// The set above with its index whole and the plain file's first article
// in: the set stands at the root, and the plain file and the recovery
// volume are still to come.
fn set() -> Set {
    let plain = payload(2 * ARTICLE, 47);
    let par2 = par2_set("plain", &[("plain.bin", &plain)], 2);
    let files = vec![
        file("plain.bin", &[ARTICLE as u32, ARTICLE as u32]),
        file(&par2[0].name, &sizes(&par2[0])),
        file(&par2[1].name, &sizes(&par2[1])),
    ];
    assert!(
        sizes(&par2[1]).len() > 1,
        "the recovery volume has articles to come"
    );
    let directory = tempfile::tempdir().unwrap();
    let writer = NamespaceWriter::new(
        1,
        directory.path().to_owned(),
        accountant(),
        CapacityReader::unknown(),
    );
    let runs = Runs::new(1, 0, RunMail::default().sender(), DecodePool::unbounded());
    let mut job = GraphJob::new(
        &files,
        Box::new(Echoes {
            volumes: 1,
            ..Default::default()
        }),
        writer,
        runs,
        u32::MAX,
    );
    let index = par2[0].bytes.len() as u64;
    for (ordinal, bytes) in par2[0].articles(ARTICLE).enumerate() {
        intake(&mut job, INDEX, ordinal as u32, bytes, index);
    }
    intake(&mut job, PLAIN, 0, &plain[..ARTICLE], plain.len() as u64);
    assert_eq!(
        job.tree.recovery_nodes(NodeId::ROOT).len(),
        1,
        "a set at the root"
    );
    Set {
        job,
        files,
        par2,
        _directory: directory,
    }
}

fn the_set(job: &GraphJob) -> NodeId {
    job.tree.recovery_nodes(NodeId::ROOT)[0]
}

// The recovery volume's first article makes its member the set's own: its
// owner changes, the member is marked, and the refresh classifies it again,
// so the set is what is to come for it. Lent out for its work, the set
// answers no recovery face and the volume is payload; back, it is the
// set's again. Each is one classification, and no read walks.
#[test]
fn a_member_that_changes_owner_or_whose_owner_goes_out_is_classified_again() {
    let Set { mut job, par2, .. } = set();
    assert_eq!(read(&mut job), 0);
    let set = the_set(&job);
    let volume = par2[1].bytes.len() as u64;
    let first = par2[1].articles(ARTICLE).next().unwrap();
    intake(&mut job, RECOVERY, 0, first, volume);
    assert_eq!(read(&mut job), 0);
    assert!(
        job.coming().sets.contains(&set),
        "the recovery volume is the set's own"
    );
    assert!(job.coming().payload, "the plain file is still to come");

    let (before, _, _) = job.coming_counts();
    let object = job.lend_object_for_test(set).expect("the set lends");
    assert!(!job.tree.recovers(set));
    assert_eq!(
        job.coming_counts().0,
        before + 1,
        "its one own volume to come"
    );
    assert!(job.coming().sets.is_empty(), "lent out, it answers no set");
    assert_eq!(read(&mut job), 0);

    job.take_back_for_test(set, object, 0).unwrap();
    assert_eq!(read(&mut job), 0);
    assert!(
        job.coming().sets.contains(&set),
        "back, the volume is its own"
    );
    assert_eq!(job.coming_counts().0, before + 2);
}

// A restored job keeps no answer: its first refresh classifies each volume
// to come once, the plain file and the recovery volume, and no read walks.
#[test]
fn a_restored_job_classifies_each_volume_to_come_once() {
    let Set {
        job,
        files,
        _directory: directory,
        ..
    } = set();
    let checkpoint = Pending::default();
    assert!(checkpoint.put(&job, Vec::new()));
    let snapshot = checkpoint::snapshot(&rows(checkpoint.take().0.unwrap())).unwrap();
    drop(job);
    let mail = RunMail::default();
    let mut restored = GraphJob::restore(
        &snapshot,
        &files,
        1,
        Storage {
            working_dir: directory.path().to_owned(),
            output: None,
            accountant: accountant(),
            decode: DecodePool::unbounded(),
            free_disk: CapacityReader::unknown(),
            slots: 1,
            mail: mail.sender(),
            passwords: Vec::new(),
            caps: Caps::unbounded(),
            nesting: u32::MAX,
            hardware: Default::default(),
            metrics: None,
            data_dir: None,
            window_ledger: Default::default(),
            disk: None,
        },
        0,
    )
    .unwrap()
    .job;
    assert_eq!(read(&mut restored), 0);
    assert_eq!(restored.coming_counts().0, 2);
    let set = the_set(&restored);
    assert!(restored.coming().payload && restored.coming().sets.contains(&set));
}

// The root namespace replaced: its generation goes back, so a read before
// the job brings the answer up to date walks, and the next refresh makes
// the answer again from every volume to come, none of them a member the new
// namespace holds.
#[test]
fn a_replaced_root_namespace_is_read_by_the_walk_until_the_answer_is_made_again() {
    let Set { mut job, .. } = set();
    assert_eq!(read(&mut job), 0);
    let (before, _, _) = job.coming_counts();
    *job.namespace_mut(NodeId::ROOT).unwrap() = Namespace::new();
    let coming = job.coming();
    assert_eq!(job.coming_counts(), (before, 1, 0), "the stale read walks");
    assert!(!coming.payload && coming.sets.is_empty());
    job.phase_change();
    assert_eq!(
        job.coming_counts().0,
        before + 2,
        "both volumes to come again"
    );
    let _ = job.coming();
    assert_eq!(
        job.coming_counts(),
        (before + 2, 1, 0),
        "and the next read is fresh"
    );
}
