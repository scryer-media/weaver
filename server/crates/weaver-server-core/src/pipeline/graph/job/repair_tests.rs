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
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::tests::{decoded, file};
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::tests::engines::harness::{ARTICLE, par2_set, payload};

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
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let runs = Runs::new(1, 0, RunMail::default().sender(), accountant);
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
