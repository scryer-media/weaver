// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::engine::{RunMail, Runs, echo::Echoes};
use crate::pipeline::graph::envelope::Shelf;
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::tests::{decoded, file};
use crate::pipeline::graph::status::RepairSlots;
use std::sync::Arc;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};

// What a run's read of volume 1's length leaves on the shelf: the closed
// length, when there is one.
fn read_length(shelf: &Shelf) -> Option<u64> {
    shelf.update(|state| {
        let kept = state.kept.get_mut(&1).unwrap();
        kept.length_read = kept.closed;
        kept.closed
    })
}

// A job of `count` echo volumes of `size` bytes each, every one in and
// closed, its engine's shelf fed and no run started. The directory is the
// job's working directory.
fn echo_job(count: u32, size: u32) -> (GraphJob, NodeId, Arc<Shelf>, tempfile::TempDir) {
    let files: Vec<_> = (0..count)
        .map(|index| file(&format!("set.part{}.rar", index + 1), &[size]))
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
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let mail = RunMail::default();
    let runs = Runs::new(1, 0, mail.sender(), accountant);
    let mut job = GraphJob::new(
        &files,
        Box::new(Echoes {
            volumes: count,
            ..Default::default()
        }),
        writer,
        runs,
        u32::MAX,
    );
    for volume in 0..count {
        let mut bytes = vec![0x80; size as usize];
        if volume == 0 {
            bytes[..8].copy_from_slice(crate::pipeline::graph::engine::echo::SIGNATURE);
        }
        job.intake(
            decoded(volume, 0, 0, &bytes, Some(u64::from(size))),
            &mut RepairSlots::default(),
            1,
        )
        .unwrap();
    }
    let node = job.tree.engines()[0];
    feed(&mut job, node);
    job.runs.request_stop();
    let shelf = job.writer.shelf(node);
    (job, node, shelf, directory)
}

// Volume `index` of the root closes, or reopens, and the shelf is fed.
fn set_closed(job: &mut GraphJob, node: NodeId, index: u32, closed: bool) {
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(MemberId(index))
        .unwrap()
        .closed = closed;
    feed(job, node);
}

// The engine's shelf fed what the root tells it now.
fn feed(job: &mut GraphJob, node: NodeId) {
    let facts = job.facts(NodeId::ROOT, &mut None);
    job.feed(node, facts).unwrap();
}

#[tokio::test]
async fn a_reopened_volume_that_closes_at_another_length_retracts_the_run() {
    reopened_volume_closes_at(1200).await;
}

#[tokio::test]
async fn a_reopened_volume_that_closes_at_the_same_length_keeps_the_run() {
    reopened_volume_closes_at(1000).await;
}

// Volume 2 closed at 1000 and a run read on into volume 3. Volume 2 reopens,
// which moves nothing, then closes at `length`: only another length moves
// the offsets the run read past it.
async fn reopened_volume_closes_at(length: u64) {
    let (mut job, node, shelf, _directory) = echo_job(3, 1000);
    let generation = shelf.begin();
    shelf.update(|state| {
        assert_eq!(state.kept[&1].closed, Some(1000));
        state.kept.get_mut(&2).unwrap().reached = 10;
    });
    set_closed(&mut job, node, 1, false);
    assert_eq!(shelf.generation(), generation, "a reopen moves nothing");
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .recognise(MemberId(1), "set.part2.rar", length);
    set_closed(&mut job, node, 1, true);
    assert_eq!(
        shelf.generation() != generation,
        length != 1000,
        "closed again at {length}"
    );
}

#[tokio::test]
async fn only_a_changed_observed_closed_length_invalidates_the_run() {
    for (observe, initially_closed, change, invalidated) in [
        (false, true, true, false),
        (true, true, false, false),
        (true, true, true, true),
        (true, false, true, false),
    ] {
        let (mut job, node, shelf, _directory) = echo_job(2, 48);
        if !initially_closed {
            shelf.update(|state| state.kept.get_mut(&1).unwrap().closed = None);
        }
        let generation = shelf.begin();
        if observe {
            assert_eq!(read_length(&shelf), initially_closed.then_some(48));
        }
        assert_eq!(shelf.reached(), None, "no payload byte was read");
        if change {
            job.tree.namespace_mut(NodeId::ROOT).unwrap().recognise(
                MemberId(1),
                "set.part2.rar",
                47,
            );
        }
        feed(&mut job, node);
        assert_eq!(
            shelf.generation() == generation,
            !invalidated,
            "observed={observe}, initially_closed={initially_closed}, changed={change}"
        );
        if observe && initially_closed && !change {
            set_closed(&mut job, node, 1, false);
            set_closed(&mut job, node, 1, true);
            assert_eq!(
                shelf.generation(),
                generation,
                "a reopen at the same length moves nothing"
            );
            set_closed(&mut job, node, 1, false);
            job.tree.namespace_mut(NodeId::ROOT).unwrap().recognise(
                MemberId(1),
                "set.part2.rar",
                47,
            );
            set_closed(&mut job, node, 1, true);
            assert_eq!(
                shelf.generation(),
                generation + 1,
                "the read length did not survive the reopen"
            );
            feed(&mut job, node);
            assert_eq!(
                shelf.generation(),
                generation + 1,
                "the consumed observation cannot invalidate repeatedly"
            );
        }
    }
}

// An article of one archive brings up that archive's engine alone: of the
// fifty engines' shelves, only its own is locked, and it is fed what its
// volume now holds.
#[test]
fn an_article_locks_only_the_shelf_of_the_engine_it_reached() {
    use super::super::repair::tests::{Archives, archive_tail, archives};
    use crate::pipeline::graph::envelope::LOCKED;

    let Archives { mut job, .. } = archives();
    let engine = job.tree.child_of(NodeId::ROOT, MemberId(7)).unwrap().0;
    LOCKED.take();

    archive_tail(&mut job, 7);

    let locked: std::collections::BTreeSet<NodeId> = LOCKED.take().into_iter().collect();
    assert_eq!(locked, [engine].into());
    let shelf = job.writer.shelf(engine);
    assert_eq!(shelf.lock().kept[&0].closed, Some(2 * ARTICLE_BYTES));
}

const ARTICLE_BYTES: u64 = crate::pipeline::tests::engines::harness::ARTICLE as u64;
