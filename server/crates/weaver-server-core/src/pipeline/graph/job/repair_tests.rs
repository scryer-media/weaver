// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::cell::RefCell;
use std::sync::Arc;

use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{MemberId, NodeId};

use super::super::GraphJob;
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::engine::echo::{Echoes, SIGNATURE};
use crate::pipeline::graph::engine::{RunMail, Runs};
use crate::pipeline::graph::hardware::Hardware;
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::tests::{decoded, file};
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::tests::engines::harness::{ARTICLE, par2_set, payload};
use crate::pipeline::tests::engines::loans::{
    FRAME, Kind, SECRET, body, stored_zip, volumes, zipcrypto_descriptor,
};

thread_local! {
    // The namespaces the walk asked to rule, in the order it asked.
    pub(super) static RULED: RefCell<Vec<NodeId>> = const { RefCell::new(Vec::new()) };
    // The root members a seal pass asked whether the sets proved them.
    pub(super) static SEAL_ASKED: RefCell<Vec<MemberId>> = const { RefCell::new(Vec::new()) };
}

// What `archives` built: the job, the directory its files are in, and the
// plain file's bytes.
pub(in crate::pipeline::graph::job) struct Archives {
    pub(in crate::pipeline::graph::job) job: GraphJob,
    pub(in crate::pipeline::graph::job) plain: Vec<u8>,
    _directory: tempfile::TempDir,
}

// The plain file's root member and volume in `Archives`: the archives come
// first, then the plain file, then the PAR2 index over it.
pub(in crate::pipeline::graph::job) const PLAIN: u32 = 50;

// A job of fifty one-volume echo archives of two articles each, a plain
// file of two articles and a PAR2 index over the plain file. Every
// archive's first article is in, so each has its engine; the index is
// whole, so the root has a set; and the plain file's first article is in.
// The slot limit is zero throughout, so no run starts and no repair goes
// out.
pub(in crate::pipeline::graph::job) fn archives() -> Archives {
    let article = ARTICLE as u32;
    let plain = payload(2 * ARTICLE, 31);
    let par2 = par2_set("plain", &[("plain.bin", &plain)], 2);
    let index = &par2[0].bytes;
    let mut files: Vec<_> = (0..PLAIN)
        .map(|archive| file(&format!("e{archive}.rar"), &[article, article]))
        .collect();
    files.push(file("plain.bin", &[article, article]));
    let index_sizes: Vec<u32> = par2[0].articles(ARTICLE).map(|a| a.len() as u32).collect();
    files.push(file("plain.par2", &index_sizes));
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
    let runs = Runs::new(
        1,
        0,
        RunMail::default().sender(),
        crate::pipeline::graph::engine::DecodePool::unbounded(),
    );
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
    let mut slots = RepairSlots::default();
    for archive in 0..PLAIN {
        let mut front = vec![0x80; ARTICLE];
        front[..SIGNATURE.len()].copy_from_slice(SIGNATURE);
        let length = Some(2 * ARTICLE as u64);
        job.intake(decoded(archive, 0, 0, &front, length), &mut slots, 0)
            .unwrap();
    }
    for (ordinal, bytes) in par2[0].articles(ARTICLE).enumerate() {
        let offset = (ordinal * ARTICLE) as u64;
        let length = Some(index.len() as u64);
        let volume = PLAIN + 1;
        job.intake(
            decoded(volume, ordinal as u32, offset, bytes, length),
            &mut slots,
            0,
        )
        .unwrap();
    }
    let length = Some(plain.len() as u64);
    job.intake(
        decoded(PLAIN, 0, 0, &plain[..ARTICLE], length),
        &mut slots,
        0,
    )
    .unwrap();
    assert_eq!(
        job.tree.engines().len(),
        PLAIN as usize,
        "an engine per archive"
    );
    assert_eq!(
        job.tree.recovery_nodes(NodeId::ROOT).len(),
        1,
        "a set at the root"
    );
    Archives {
        job,
        plain,
        _directory: directory,
    }
}

// The second article of archive `archive`.
pub(in crate::pipeline::graph::job) fn archive_tail(job: &mut GraphJob, archive: u32) {
    let tail = vec![0x81; ARTICLE];
    let length = Some(2 * ARTICLE as u64);
    job.intake(
        decoded(archive, 1, ARTICLE as u64, &tail, length),
        &mut RepairSlots::default(),
        0,
    )
    .unwrap();
}

// An article of the plain file rules the one namespace that has a set,
// the root, and asks none of the fifty archives' namespaces, which have
// none to rule.
#[test]
fn an_article_rules_only_the_namespaces_with_sets() {
    let Archives { mut job, plain, .. } = archives();
    assert!(job.tree.live().len() > PLAIN as usize);
    RULED.take();

    let length = Some(plain.len() as u64);
    job.intake(
        decoded(PLAIN, 1, ARTICLE as u64, &plain[ARTICLE..], length),
        &mut RepairSlots::default(),
        0,
    )
    .unwrap();

    assert_eq!(RULED.take(), vec![NodeId::ROOT]);
}

// An article of one archive asks only that archive's root member whether
// the sets proved it: nothing else at the root changed.
#[test]
fn an_article_asks_only_its_own_member_whether_it_was_proved() {
    let Archives { mut job, .. } = archives();
    SEAL_ASKED.take();

    archive_tail(&mut job, 7);

    let asked = SEAL_ASKED.take();
    assert!(!asked.is_empty());
    assert!(
        asked.iter().all(|member| *member == MemberId(7)),
        "{asked:?}"
    );
}

// A ZipCrypto member whose sizes wait in a data descriptor is placed only
// as far as its keys reach, so the bytes past them wait, and the keys run
// over each part the zip holds as work. Inside a stored outer zip the outer
// member's bytes reach the inner zip only as the writer lets them go: each
// round of work lets the next part go, which the inner zip holds in turn.
// While a checkpoint is owed the work of a job that may run, one slot to
// run in, is done here, and it goes on until nothing is due: none is left
// for a gate that rules without lending it.
fn owed_rows(nested: bool, backwards: bool) {
    let inner = zipcrypto_descriptor(&body());
    let archive = match nested {
        true => stored_zip("lantern.zip", &inner),
        false => inner,
    };
    let sizes: Vec<u32> = archive
        .chunks(FRAME)
        .map(|chunk| chunk.len() as u32)
        .collect();
    let files = vec![file("outer.zip", &sizes)];
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
    let runs = Runs::new(
        1,
        0,
        RunMail::default().sender(),
        crate::pipeline::graph::engine::DecodePool::unbounded(),
    );
    let mut job = GraphJob::new(
        &files,
        Box::new(Hardware::default()),
        writer,
        runs,
        u32::MAX,
    )
    .offering(vec![SECRET.to_string()]);
    job.owe_checkpoint();
    let length = Some(archive.len() as u64);
    let mut slots = RepairSlots::default();
    let mut articles: Vec<(usize, &[u8])> = archive.chunks(FRAME).enumerate().collect();
    if backwards {
        articles.reverse();
    }
    for (ordinal, chunk) in articles {
        let offset = (ordinal * FRAME) as u64;
        job.intake(
            decoded(0, ordinal as u32, offset, chunk, length),
            &mut slots,
            1,
        )
        .unwrap();
        assert!(
            job.tree.working().is_empty(),
            "work left due after article {ordinal}"
        );
    }
    assert_eq!(job.phase(), RootPhase::Done, "the member delivered");
}

#[test]
fn owed_work_on_a_flat_zip_in_order_leaves_none_due() {
    owed_rows(false, false)
}

#[test]
fn owed_work_on_a_flat_zip_served_backwards_leaves_none_due() {
    owed_rows(false, true)
}

#[test]
fn owed_work_on_a_nested_zip_in_order_leaves_none_due() {
    owed_rows(true, false)
}

#[test]
fn owed_work_on_a_nested_zip_served_backwards_leaves_none_due() {
    owed_rows(true, true)
}

// A ZipCrypto member downloaded by a job with no slot to run its keys in.
// The tail comes first, so the zip knows the entry as its data comes: the
// first article proves the password where it arrives, and past it the zip
// holds no more than its cap of ciphertext for the keys; the rest waits in
// the holds ledger, deciphered nowhere. The last article closes the volume,
// which hears what the zip holds before anything else, as it must, and the
// cap still stands. Granted a slot, the keys run over what the zip holds,
// the ledger hands back the rest as they reach it, and the member comes
// whole. The entry is larger than the tail the zip keeps to find its
// directory, and its articles are larger than the loans' rows', so they
// are few.
#[test]
fn ciphertext_past_the_cap_waits_in_the_ledger_until_a_slot_comes() {
    use weaver_objects::zip::counting;
    const ARTICLE: usize = 16 * 1024;
    const CAP: u64 = 4 * ARTICLE as u64;
    counting::cap_here(CAP);
    let plain = payload(32 * ARTICLE + 77, 67);
    let [(name, archive)] = <[_; 1]>::try_from(volumes(Kind::ZipCryptoSized, &plain, 1)).unwrap();
    let sizes: Vec<u32> = archive
        .chunks(ARTICLE)
        .map(|chunk| chunk.len() as u32)
        .collect();
    let files = vec![file(&name, &sizes)];
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
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let runs = Runs::new(1, 0, RunMail::default().sender(), accountant);
    let mut job = GraphJob::new(
        &files,
        Box::new(Hardware::default()),
        writer,
        runs,
        u32::MAX,
    )
    .offering(vec![SECRET.to_string()]);
    let length = Some(archive.len() as u64);
    let mut slots = RepairSlots::default();
    let deciphered = counting::deciphered_here();
    let mut articles: Vec<(usize, &[u8])> = archive.chunks(ARTICLE).enumerate().collect();
    // The tail first, as far as the zip looks for its directory.
    articles.rotate_right(5);
    let last = articles.pop().unwrap();
    let mut intake = |job: &mut GraphJob, (ordinal, chunk): (usize, &[u8])| {
        let offset = (ordinal * ARTICLE) as u64;
        job.intake(
            decoded(0, ordinal as u32, offset, chunk, length),
            &mut slots,
            0,
        )
        .unwrap();
    };
    for article in articles {
        intake(&mut job, article);
    }
    let proved = counting::deciphered_here() - deciphered;
    assert!(
        proved <= ARTICLE as u64,
        "deciphered with no slot: {proved}"
    );
    assert_eq!(counting::queued_most_here(), CAP, "held for the keys");
    let held = job.writer.held_bytes(NodeId::ROOT);
    assert!(
        held >= plain.len() as u64 - ARTICLE as u64 - CAP - last.1.len() as u64,
        "the ledger holds {held} of {}",
        plain.len()
    );
    assert!(!job.tree.working().is_empty(), "the keys' work waits");
    intake(&mut job, last);
    assert!(counting::queued_most_here() <= CAP);
    assert_ne!(job.phase(), RootPhase::Done);

    // A slot: the work is done here, as while a checkpoint is owed.
    job.owe_checkpoint();
    job.lend(&mut slots, 1).unwrap();
    assert!(job.tree.working().is_empty(), "work left due");
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0, "left in the ledger");
    assert_eq!(job.phase(), RootPhase::Done, "the member delivered");
    assert!(super::super::loan_tests::holds(directory.path(), &plain));
    assert!(counting::queued_most_here() <= CAP);
}
